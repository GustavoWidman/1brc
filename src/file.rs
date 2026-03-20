use memmap2::Mmap;

use crate::{
    NUM_WORKERS,
    hashmap::HashMap,
    measurement::FinalMeasurement,
};

pub struct File {
    mmap: Mmap,
}

impl<'a> File {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        // Use FILE_FLAG_SEQUENTIAL_SCAN on Windows for optimized readahead
        #[cfg(target_os = "windows")]
        let file = {
            use std::os::windows::fs::OpenOptionsExt;
            std::fs::File::options()
                .read(true)
                .custom_flags(0x08000000) // FILE_FLAG_SEQUENTIAL_SCAN
                .open(path)?
        };
        #[cfg(not(target_os = "windows"))]
        let file = std::fs::File::options().read(true).open(path)?;

        let mmap = unsafe { memmap2::MmapOptions::new().huge(None).map(&file)? };
        Ok(Self { mmap })
    }

    /// Parse temperature from raw pointer using SWAR — minimal branches.
    /// Handles: D.D, DD.D, -D.D, -DD.D (standard 1BRC formats).
    #[inline(always)]
    unsafe fn parse_temp(tp_start: *const u8) -> (i16, *const u8) {
        unsafe {
            let word = (tp_start as *const u64).read_unaligned();

            // Negative sign check — branchless shift
            let b0 = (word & 0xFF) as u8;
            let neg_bit = (b0 == b'-') as u64;
            let w = word >> (neg_bit << 3);

            // Check if byte[1] is '.' (short form: D.D) or a digit (long form: DD.D)
            let dot_at_1 = ((w >> 8) & 0xFF) == 0x2E;

            // Extract digits — compute both forms, select the right one
            // Use wrapping_sub because non-digit positions will overflow (and be discarded)
            let b0d = ((w & 0xFF).wrapping_sub(b'0' as u64)) as i16;
            let b1d = (((w >> 8) & 0xFF).wrapping_sub(b'0' as u64)) as i16;
            let b2d = (((w >> 16) & 0xFF).wrapping_sub(b'0' as u64)) as i16;
            let b3d = (((w >> 24) & 0xFF).wrapping_sub(b'0' as u64)) as i16;

            // Short: D.D -> b0d * 10 + b2d, advance 4 + neg
            // Long:  DD.D -> b0d * 100 + b1d * 10 + b3d, advance 5 + neg
            let (val, advance) = if dot_at_1 {
                (b0d * 10 + b2d, 4 + neg_bit as usize)
            } else {
                (b0d * 100 + b1d * 10 + b3d, 5 + neg_bit as usize)
            };

            // Apply sign — branchless via multiply
            let sign = 1 - 2 * neg_bit as i16;
            (val * sign, tp_start.add(advance))
        }
    }

    /// Find byte `needle` starting from `ptr`, scanning up to `max_len` bytes.
    /// Returns offset from ptr, or max_len if not found.
    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    unsafe fn find_byte_simd(ptr: *const u8, max_len: usize, needle: u8) -> usize {
        use std::arch::x86_64::*;

        unsafe {
            let needle_vec = _mm256_set1_epi8(needle as i8);
            let mut offset: usize = 0;

            // AVX2 path: 32 bytes at a time
            while offset + 32 <= max_len {
                let chunk = _mm256_loadu_si256(ptr.add(offset) as *const __m256i);
                let cmp = _mm256_cmpeq_epi8(chunk, needle_vec);
                let mask = _mm256_movemask_epi8(cmp) as u32;
                if mask != 0 {
                    return offset + mask.trailing_zeros() as usize;
                }
                offset += 32;
            }

            // Scalar fallback for remaining bytes
            while offset < max_len {
                if *ptr.add(offset) == needle {
                    return offset;
                }
                offset += 1;
            }
        }

        max_len
    }

    /// Find byte `needle` using NEON — 16 bytes at a time.
    #[inline(always)]
    #[cfg(target_arch = "aarch64")]
    unsafe fn find_byte_simd(ptr: *const u8, max_len: usize, needle: u8) -> usize {
        use std::arch::aarch64::*;

        unsafe {
            let needle_vec = vdupq_n_u8(needle);
            let mut offset: usize = 0;

            // NEON path: 16 bytes at a time
            while offset + 16 <= max_len {
                let chunk = vld1q_u8(ptr.add(offset));
                let cmp = vceqq_u8(chunk, needle_vec);
                // Narrow to 8-bit saturated, then reinterpret as u64 pair
                let narrowed = vshrn_n_u16(vreinterpretq_u16_u8(cmp), 4);
                let bits = vget_lane_u64(vreinterpret_u64_u8(narrowed), 0);
                if bits != 0 {
                    return offset + (bits.trailing_zeros() as usize >> 2);
                }
                offset += 16;
            }

            // Scalar fallback
            while offset < max_len {
                if *ptr.add(offset) == needle {
                    return offset;
                }
                offset += 1;
            }
        }

        max_len
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2,bmi1,bmi2")]
    unsafe fn parse_buffer(data: &'a [u8]) -> HashMap<'a> {
        unsafe {
            let mut result = HashMap::new();
            let mut pos = 0;
            let len = data.len();
            let base = data.as_ptr();

            while pos < len {
                let offset = Self::find_byte_simd(base.add(pos), len - pos, b';');
                if offset >= len - pos { break; }
                let semi = pos + offset;

                let name = std::slice::from_raw_parts(base.add(pos), semi - pos);

                // Prefetch hash table slot while we parse the temperature
                let hash = result.prefetch_slot(name);

                // Parse temperature (gives time for prefetch to complete)
                let (val, next_ptr) = Self::parse_temp(base.add(semi + 1));

                pos = next_ptr.offset_from(base) as usize;

                // Insert with pre-computed hash (slot should be warm now)
                result.insert_with_hash(name, val, hash);
            }

            result
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "crc,neon")]
    unsafe fn parse_buffer(data: &'a [u8]) -> HashMap<'a> {
        unsafe {
            let mut result = HashMap::new();
            let mut pos = 0;
            let len = data.len();
            let base = data.as_ptr();

            while pos < len {
                let offset = Self::find_byte_simd(base.add(pos), len - pos, b';');
                if offset >= len - pos { break; }
                let semi = pos + offset;

                let name = std::slice::from_raw_parts(base.add(pos), semi - pos);

                // Prefetch hash table slot while we parse the temperature
                let hash = result.prefetch_slot(name);

                // Parse temperature (gives time for prefetch to complete)
                let (val, next_ptr) = Self::parse_temp(base.add(semi + 1));

                pos = next_ptr.offset_from(base) as usize;

                // Insert with pre-computed hash (slot should be warm now)
                result.insert_with_hash(name, val, hash);
            }

            result
        }
    }

    pub fn parse(&self) -> Vec<(String, FinalMeasurement)> {
        let chunks = self.chunk_file();

        // Process chunks in parallel using std::thread::scope
        let chunk_results: Vec<HashMap<'_>> = std::thread::scope(|s| {
            let handles: Vec<_> = chunks
                .into_iter()
                .map(|chunk| s.spawn(move || unsafe { Self::parse_buffer(chunk) }))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        // Merge results — take the first as base, merge rest into it
        let mut iter = chunk_results.into_iter();
        let mut measurements = iter.next().unwrap_or_else(HashMap::new);
        for chunk_map in iter {
            measurements.merge(chunk_map);
        }

        let mut results: Vec<(String, FinalMeasurement)> = measurements
            .into_iter()
            .map(|(city, measurement)| {
                let final_measurement: FinalMeasurement = measurement.into();
                let city = unsafe { std::str::from_utf8_unchecked(city) }.to_string();
                (city, final_measurement)
            })
            .collect();

        results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        results
    }

    fn chunk_file(&self) -> Vec<&[u8]> {
        let buffer = &self.mmap[..];
        let total_size = buffer.len();

        if total_size == 0 {
            return vec![];
        }

        let num_chunks = NUM_WORKERS;
        let chunk_size = total_size / num_chunks;

        // For very small files, don't bother with multiple chunks
        if chunk_size < 4096 {
            return vec![buffer];
        }

        let mut chunks = Vec::with_capacity(num_chunks + 1);
        let mut start = 0;

        while start < total_size {
            let mut end = (start + chunk_size).min(total_size);

            if end < total_size {
                // Find next newline to align chunk boundary
                while end < total_size && buffer[end] != b'\n' {
                    end += 1;
                }
                if end < total_size {
                    end += 1; // include the newline
                }
            }

            chunks.push(&buffer[start..end]);
            start = end;
        }

        chunks
    }
}
