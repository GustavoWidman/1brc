use hashbrown::hash_map::RawEntryMut;
use memmap2::Mmap;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    NUM_WORKERS,
    hashmap::HashMap,
    measurement::{FinalMeasurement, Measurement},
};

pub struct File {
    mmap: Arc<Mmap>,
}

impl<'a> File {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let file = std::fs::File::options().read(true).open(path)?;
        Ok(Self {
            mmap: Arc::new(unsafe { memmap2::MmapOptions::new().huge(None).map(&file)? }),
        })
    }

    fn parse_buffer(data: &'a [u8]) -> HashMap<'a> {
        let mut result = HashMap::new();
        let mut buffer = &data[..];

        loop {
            match memchr::memchr(b';', &buffer) {
                None => {
                    break;
                }
                Some(comma_separator) => {
                    let end = memchr::memchr(b'\n', &buffer[comma_separator..]).unwrap();
                    let name = &buffer[..comma_separator];
                    let value_bytes = &buffer[comma_separator + 1..comma_separator + end];
                    let value = Self::parse_fake_float(value_bytes);

                    match result.raw_entry_mut().from_key(name) {
                        RawEntryMut::Occupied(mut entry) => {
                            entry.get_mut().add(value);
                        }
                        RawEntryMut::Vacant(entry) => {
                            entry.insert(name, Measurement::new(value));
                        }
                    }

                    buffer = &buffer[comma_separator + end + 1..];
                }
            }
        }

        result
    }

    #[inline(always)]
    fn parse_fake_float(mut bytes: &[u8]) -> i16 {
        let negative = unsafe { *bytes.get_unchecked(0) } == b'-';

        if negative {
            // Only parse digits.
            bytes = &unsafe { bytes.get_unchecked(1..) };
        }

        let mut val = 0;
        for &byte in bytes {
            if byte == b'.' {
                continue;
            }
            let digit = (byte - b'0') as i16;
            val = val * 10 + digit;
        }

        if negative { -val } else { val }
    }

    pub fn parse(&self) -> BTreeMap<String, FinalMeasurement> {
        let chunks = self.chunk_file();

        let measurements = chunks
            .into_par_iter()
            .map(|chunk| Self::parse_buffer(chunk))
            .reduce(
                || HashMap::new(),
                |mut left, right| {
                    left.merge(right);
                    left
                },
            );

        let final_measurements = measurements
            .into_inner()
            .into_par_iter()
            .map(|(city, measurement)| {
                let final_measurement: FinalMeasurement = measurement.into();
                let city = String::from_utf8_lossy(city).to_string();
                (city, final_measurement)
            })
            .collect::<BTreeMap<String, FinalMeasurement>>();

        final_measurements
    }

    fn chunk_file(&self) -> Vec<&[u8]> {
        let buffer = &self.mmap[..];
        let total_size = buffer.len();

        // Handle small files - no need to chunk
        if total_size < 1024 * 1024 {
            // Less than 1MB
            return vec![buffer];
        }

        let chunk_size = total_size / NUM_WORKERS;
        let mut chunks = Vec::with_capacity(NUM_WORKERS);
        let mut start = 0;

        while start < total_size {
            let mut end = (start + chunk_size).min(total_size);

            // Don't split in the middle of a line - search forward for newline
            if end < total_size {
                while end < total_size && buffer[end] != b'\n' {
                    end += 1;
                }
                // Include the newline in the previous chunk
                end += 1;
            }

            chunks.push(&buffer[start..end]);
            start = end;
        }

        chunks
    }
}
