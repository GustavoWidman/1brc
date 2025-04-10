use std::ops::{Deref, DerefMut};

use crate::STATIONS_IN_DATASET;

use super::measurement::Measurement;

pub type InnerHashMap<'a> = hashbrown::HashMap<&'a [u8], Measurement, ahash::RandomState>;

pub struct HashMap<'a> {
    inner: InnerHashMap<'a>,
}
impl<'a> HashMap<'a> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: InnerHashMap::with_capacity_and_hasher(
                STATIONS_IN_DATASET * 2,
                ahash::RandomState::new(),
            ),
        }
    }

    #[inline(always)]
    pub fn into_inner(self) -> InnerHashMap<'a> {
        self.inner
    }

    #[inline(always)]
    pub fn merge(&mut self, other: HashMap<'a>) {
        other.into_inner().into_iter().for_each(|(key, val)| {
            match self.get_mut(&key) {
                Some(m) => m.merge(&val),
                None => {
                    let _res = self.insert(key, val);
                }
            };
        });
    }
}

impl<'a> Deref for HashMap<'a> {
    type Target = InnerHashMap<'a>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> DerefMut for HashMap<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
