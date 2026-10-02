//! A `HashMap` for tag-number keys.
//!
//! The default SipHash is built to resist adversarial keys, which costs more
//! than the lookup it guards. Tag numbers come from the dictionary, not from the
//! peer, so a multiplicative hash (the one rustc uses internally) is enough.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default, Clone, Copy)]
pub(crate) struct TagHasher(u64);

impl Hasher for TagHasher {
  fn finish(&self) -> u64 {
    self.0
  }

  fn write(&mut self, bytes: &[u8]) {
    for b in bytes {
      self.write_u64(*b as u64);
    }
  }

  fn write_u32(&mut self, n: u32) {
    self.write_u64(n as u64);
  }

  fn write_u64(&mut self, n: u64) {
    self.0 =
      (self.0.rotate_left(5) ^ n).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
  }
}

pub(crate) type TagMap<V> = HashMap<u32, V, BuildHasherDefault<TagHasher>>;
