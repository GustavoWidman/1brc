use rayon::prelude::{IntoParallelIterator, ParallelIterator, ParallelSliceMut};
use std::{
    simd::{prelude::SimdPartialEq, u8x64},
    sync::Arc,
};

use crate::{
    hashmap::HashMap,
    measurement::{FinalMeasurement, Measurement},
};

pub struct File {
    mmap: Arc<memmap2::Mmap>,
}

static NUM_WORKERS: usize = 14;

impl<'a> File {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let file = std::fs::File::options().read(true).open(path)?;
        Ok(Self {
            mmap: Arc::new(unsafe { memmap2::Mmap::map(&file)? }),
        })
    }

    // fn parse_buffer(buffer: &'a [u8]) -> HashMap<'a> {
    //     let mut result = HashMap::new();
    //     buffer.split(|byte: &u8| *byte == b'\n').for_each(|line| {
    //         let mut parts = line.split(|byte: &u8| *byte == b';');
    //         if let (Some(city), Some(value)) = (parts.next(), parts.next()) {
    //             if let Some(value) = fast_float2::parse(value).ok() {
    //                 result
    //                     .entry(city)
    //                     .and_modify(|m| m.add(value))
    //                     .or_insert(Measurement::new(value));
    //             }
    //         }
    //     });

    //     result
    // }

    fn parse_buffer(buffer: &[u8]) -> HashMap<'_> {
        let mut result = HashMap::new();
        let mut pos = 0;
        let len = buffer.len();

        while pos < len {
            let remaining = len - pos;
            let mut newline_pos = None;

            let mut offset = 0;
            while remaining - offset >= 256 {
                let base = pos + offset;
                let b0 = u8x64::from_slice(&buffer[base..base + 64]);
                let b1 = u8x64::from_slice(&buffer[base + 64..base + 128]);
                let b2 = u8x64::from_slice(&buffer[base + 128..base + 192]);
                let b3 = u8x64::from_slice(&buffer[base + 192..base + 256]);
                let needle = u8x64::splat(b'\n');

                let m0 = b0.simd_eq(needle).to_bitmask();
                let m1 = b1.simd_eq(needle).to_bitmask();
                let m2 = b2.simd_eq(needle).to_bitmask();
                let m3 = b3.simd_eq(needle).to_bitmask();

                if m0 != 0 {
                    newline_pos = Some(offset + m0.trailing_zeros() as usize);
                    break;
                } else if m1 != 0 {
                    newline_pos = Some(offset + 64 + m1.trailing_zeros() as usize);
                    break;
                } else if m2 != 0 {
                    newline_pos = Some(offset + 128 + m2.trailing_zeros() as usize);
                    break;
                } else if m3 != 0 {
                    newline_pos = Some(offset + 192 + m3.trailing_zeros() as usize);
                    break;
                }
                offset += 256;
            }

            if newline_pos.is_none() {
                let scan_start = pos + offset;
                let remaining_tail = len - scan_start;
                let chunk_len = remaining_tail.min(64);
                if chunk_len == 64 {
                    let bytes = u8x64::from_slice(&buffer[scan_start..scan_start + 64]);
                    let needle = u8x64::splat(b'\n');
                    let mask = bytes.simd_eq(needle);
                    let bitmask = mask.to_bitmask();
                    if bitmask != 0 {
                        newline_pos = Some(offset + bitmask.trailing_zeros() as usize);
                    }
                } else {
                    for i in 0..chunk_len {
                        if buffer[scan_start + i] == b'\n' {
                            newline_pos = Some(offset + i);
                            break;
                        }
                    }
                }
            }

            let line_end = match newline_pos {
                Some(rel_pos) => pos + rel_pos,
                None => len,
            };

            let mut semi_pos = None;
            let line = &buffer[pos..line_end];
            let line_len = line.len();
            let mut offset = 0;
            while line_len - offset >= 256 {
                let base = offset;
                let b0 = u8x64::from_slice(&line[base..base + 64]);
                let b1 = u8x64::from_slice(&line[base + 64..base + 128]);
                let b2 = u8x64::from_slice(&line[base + 128..base + 192]);
                let b3 = u8x64::from_slice(&line[base + 192..base + 256]);
                let needle = u8x64::splat(b';');

                let m0 = b0.simd_eq(needle).to_bitmask();
                let m1 = b1.simd_eq(needle).to_bitmask();
                let m2 = b2.simd_eq(needle).to_bitmask();
                let m3 = b3.simd_eq(needle).to_bitmask();

                if m0 != 0 {
                    semi_pos = Some(offset + m0.trailing_zeros() as usize);
                    break;
                } else if m1 != 0 {
                    semi_pos = Some(offset + 64 + m1.trailing_zeros() as usize);
                    break;
                } else if m2 != 0 {
                    semi_pos = Some(offset + 128 + m2.trailing_zeros() as usize);
                    break;
                } else if m3 != 0 {
                    semi_pos = Some(offset + 192 + m3.trailing_zeros() as usize);
                    break;
                }
                offset += 256;
            }

            if semi_pos.is_none() {
                while offset < line_len {
                    let rem = line_len - offset;
                    let clen = rem.min(64);
                    if clen == 64 {
                        let bytes = u8x64::from_slice(&line[offset..offset + 64]);
                        let needle = u8x64::splat(b';');
                        let mask = bytes.simd_eq(needle);
                        let bitmask = mask.to_bitmask();
                        if bitmask != 0 {
                            semi_pos = Some(offset + bitmask.trailing_zeros() as usize);
                            break;
                        }
                    } else {
                        for j in 0..clen {
                            if line[offset + j] == b';' {
                                semi_pos = Some(offset + j);
                                break;
                            }
                        }
                        if semi_pos.is_some() {
                            break;
                        }
                    }
                    offset += clen;
                }
            }

            if let Some(semi_idx) = semi_pos {
                let city = &line[..semi_idx];
                let value_bytes = &line[semi_idx + 1..];
                if let Some(value) = fast_float2::parse(value_bytes).ok() {
                    result
                        .entry(city)
                        .and_modify(|m| m.add(value))
                        .or_insert(Measurement::new(value));
                }
            }

            pos = match newline_pos {
                Some(rel_pos) => pos + rel_pos + 1,
                None => len,
            };
        }

        result
    }

    pub fn parse(&self) -> Vec<(String, FinalMeasurement)> {
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

        let mut final_measurements = measurements
            .into_inner()
            .into_par_iter()
            .map(|(city, measurement)| {
                let final_measurement: FinalMeasurement = measurement.into();
                let city = String::from_utf8_lossy(city).to_string();
                (city, final_measurement)
            })
            .collect::<Vec<(String, FinalMeasurement)>>();

        final_measurements.par_sort_by(|a, b| a.0.cmp(&b.0));

        final_measurements
    }

    fn chunk_file(&self) -> [&[u8]; NUM_WORKERS] {
        let file_size = self.mmap.len();
        let chunk_size = file_size / NUM_WORKERS;
        let mut chunks = [(0, 0); NUM_WORKERS];
        let mut start: usize = 0;

        for i in 0..NUM_WORKERS - 1 {
            let end = start + (chunk_size as usize);
            match memchr::memchr(b'\n', &self.mmap[end..]) {
                Some(pos) => {
                    chunks[i] = (start, end + pos);
                    start = end + pos + 1;
                }
                None => {
                    chunks[i] = (start, file_size);
                    break;
                }
            }
        }
        chunks[NUM_WORKERS - 1] = (start, file_size);

        let mut result: [&[u8]; NUM_WORKERS] = [&[]; NUM_WORKERS];

        chunks
            .into_iter()
            .enumerate()
            .for_each(|(i, (start, end))| result[i] = &self.mmap[start..end]);

        result
    }
}
