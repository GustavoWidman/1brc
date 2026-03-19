use super::measurement::Measurement;

const CAPACITY: usize = 4096;
const MASK: usize = CAPACITY - 1;

pub struct HashMap<'a> {
    entries: Box<[Entry<'a>]>,
    pub len: usize,
}

struct Entry<'a> {
    hash: u64,
    key: &'a [u8],
    measurement: Measurement,
}

impl<'a> Default for Entry<'a> {
    #[inline(always)]
    fn default() -> Self {
        Self {
            hash: 0,
            key: &[],
            measurement: Measurement::empty(),
        }
    }
}

/// Full-content hash using CRC32C intrinsics (1 cycle/8 bytes throughput).
/// Hashes ALL bytes for collision safety, enabling hash-only key matching.
#[inline(always)]
#[cfg(target_arch = "x86_64")]
fn hash_key(key: &[u8]) -> u64 {
    use std::arch::x86_64::_mm_crc32_u64;

    let len = key.len();
    let ptr = key.as_ptr();

    unsafe {
        let crc = if len >= 16 {
            let mut h = _mm_crc32_u64(len as u64, (ptr as *const u64).read_unaligned());
            let mut i = 8usize;
            while i + 8 <= len {
                h = _mm_crc32_u64(h, (ptr.add(i) as *const u64).read_unaligned());
                i += 8;
            }
            _mm_crc32_u64(h, (ptr.add(len - 8) as *const u64).read_unaligned())
        } else if len >= 8 {
            let a = (ptr as *const u64).read_unaligned();
            let b = (ptr.add(len - 8) as *const u64).read_unaligned();
            _mm_crc32_u64(_mm_crc32_u64(len as u64, a), b)
        } else if len >= 4 {
            let lo = (ptr as *const u32).read_unaligned() as u64;
            let hi = (ptr.add(len - 4) as *const u32).read_unaligned() as u64;
            _mm_crc32_u64(len as u64, lo | (hi << 32))
        } else {
            let mut buf = [0u8; 8];
            std::ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), len);
            _mm_crc32_u64(len as u64, u64::from_ne_bytes(buf))
        };

        crc | 1
    }
}

impl<'a> HashMap<'a> {
    #[inline(always)]
    pub fn new() -> Self {
        let entries = (0..CAPACITY)
            .map(|_| Entry::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { entries, len: 0 }
    }

    /// Compute hash and prefetch the likely hash table slot.
    #[inline(always)]
    pub fn prefetch_slot(&self, key: &[u8]) -> u64 {
        let hash = hash_key(key);
        let idx = (hash as usize) & MASK;
        unsafe {
            let ptr = self.entries.as_ptr().add(idx) as *const u8;
            #[cfg(target_arch = "x86_64")]
            std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
        }
        hash
    }

    #[inline(always)]
    pub fn insert_with_hash(&mut self, key: &'a [u8], value: i16, hash: u64) {
        let mut idx = (hash as usize) & MASK;

        loop {
            let entry = unsafe { self.entries.get_unchecked_mut(idx) };

            if entry.hash == 0 {
                entry.hash = hash;
                entry.key = key;
                entry.measurement = Measurement::new(value);
                self.len += 1;
                return;
            }

            // CRC32C of all bytes + length: collision astronomically unlikely
            if entry.hash == hash {
                entry.measurement.add(value);
                return;
            }

            idx = (idx + 1) & MASK;
        }
    }

    pub fn merge(&mut self, other: HashMap<'a>) {
        let mut remaining = other.len;
        for entry in other.entries.iter() {
            if remaining == 0 { break; }
            if entry.hash == 0 { continue; }
            remaining -= 1;

            let mut idx = (entry.hash as usize) & MASK;
            loop {
                let self_entry = unsafe { self.entries.get_unchecked_mut(idx) };
                if self_entry.hash == 0 {
                    self_entry.hash = entry.hash;
                    self_entry.key = entry.key;
                    self_entry.measurement = Measurement::new_from(&entry.measurement);
                    self.len += 1;
                    break;
                }
                if self_entry.hash == entry.hash {
                    self_entry.measurement.merge(&entry.measurement);
                    break;
                }
                idx = (idx + 1) & MASK;
            }
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = (&'a [u8], Measurement)> {
        self.entries
            .into_vec()
            .into_iter()
            .filter(|e| e.hash != 0)
            .map(|e| (e.key, e.measurement))
    }
}
