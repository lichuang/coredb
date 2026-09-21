//! Uniform decoding of stored values.
//!
//! Every stored value starts with a one-byte header: low nibble = type, high
//! nibble = encoding version. What follows differs per type (postcard metadata,
//! or the hand-rolled [`StringValue`] layout), and only some types carry a
//! `version`.
//!
//! This module is the single place that interprets that header. Callers must
//! dispatch on the nibble via [`StoredType::from_header`] rather than
//! trial-deserializing each metadata type: the decoders are lenient, so a
//! try-decode chain silently misreads one type's bytes as another's (fixed
//! layout [`StringValue`] bytes decoding "successfully" as [`HashMetadata`]
//! with a bogus `expires_at`).

use crate::encoding::{
  BitmapMetadata, BloomFilterMetadata, HashMetadata, HyperLogLogMetadata, JsonMetadata,
  ListMetadata, SetMetadata, StringValue, TYPE_BITMAP, TYPE_BLOOMFILTER, TYPE_HASH,
  TYPE_HYPERLOGLOG, TYPE_JSON, TYPE_LIST, TYPE_SET, TYPE_STRING, TYPE_ZSET, ZSetMetadata,
};

/// Logical data type of a stored value, taken from the header byte's low
/// nibble so it is known without decoding the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredType {
  String,
  Hash,
  List,
  Set,
  ZSet,
  Bitmap,
  BloomFilter,
  HyperLogLog,
  Json,
}

impl StoredType {
  /// Map a header byte (or any byte whose low nibble is the type) to a type.
  pub fn from_header(header: u8) -> Option<Self> {
    match header & 0x0F {
      TYPE_STRING => Some(Self::String),
      TYPE_HASH => Some(Self::Hash),
      TYPE_LIST => Some(Self::List),
      TYPE_SET => Some(Self::Set),
      TYPE_ZSET => Some(Self::ZSet),
      TYPE_BITMAP => Some(Self::Bitmap),
      TYPE_BLOOMFILTER => Some(Self::BloomFilter),
      TYPE_HYPERLOGLOG => Some(Self::HyperLogLog),
      TYPE_JSON => Some(Self::Json),
      _ => None,
    }
  }

  /// Redis `TYPE` reply name for this type.
  pub fn as_str(&self) -> &'static str {
    match self {
      Self::String => "string",
      Self::Hash => "hash",
      Self::List => "list",
      Self::Set => "set",
      Self::ZSet => "zset",
      Self::Bitmap => "bitmap",
      Self::BloomFilter => "bloomfilter",
      Self::HyperLogLog => "hyperloglog",
      Self::Json => "json",
    }
  }

  /// Whether values of this type own sub-keys (hex `key|version|part` keys)
  /// that must be deleted alongside the metadata key.
  pub fn is_complex(&self) -> bool {
    matches!(
      self,
      Self::Hash
        | Self::List
        | Self::Set
        | Self::ZSet
        | Self::Bitmap
        | Self::BloomFilter
        | Self::HyperLogLog
    )
  }
}

/// Header-derived metadata common to every stored type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueMeta {
  pub kind: StoredType,
  /// Expiration timestamp in ms; [`crate::encoding::NO_EXPIRATION`] if none.
  pub expires_at: u64,
  /// Sub-key version for complex types; `0` for simple types, which have none.
  pub version: u64,
}

impl ValueMeta {
  /// Decode the header and the per-type fields it describes.
  ///
  /// Returns `None` for empty input, an unknown type nibble, or bytes that do
  /// not decode as the type their own header claims.
  pub fn decode(bytes: &[u8]) -> Option<Self> {
    let kind = StoredType::from_header(*bytes.first()?)?;
    let (expires_at, version) = match kind {
      StoredType::String => StringValue::deserialize(bytes)
        .ok()
        .map(|v| (v.expires_at, 0))?,
      StoredType::Hash => HashMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::List => ListMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::Set => SetMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::ZSet => ZSetMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::Bitmap => BitmapMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::BloomFilter => BloomFilterMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::HyperLogLog => HyperLogLogMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, m.version))?,
      StoredType::Json => JsonMetadata::deserialize(bytes)
        .ok()
        .map(|m| (m.expires_at, 0))?,
    };
    Some(Self {
      kind,
      expires_at,
      version,
    })
  }

  pub fn is_expired(&self, now_ms: u64) -> bool {
    self.expires_at != crate::encoding::NO_EXPIRATION && now_ms >= self.expires_at
  }
}

/// Whether a stored value is expired. Undecodable values count as live, so an
/// encoding this module does not understand is never silently dropped.
pub fn is_expired(bytes: &[u8], now_ms: u64) -> bool {
  ValueMeta::decode(bytes).is_some_and(|meta| meta.is_expired(now_ms))
}

/// Re-encode `bytes` with a new expiration timestamp, preserving everything else.
///
/// Returns `None` if the value does not decode, so callers can surface an error
/// rather than overwrite a value they do not understand.
pub fn with_expires_at(bytes: &[u8], expires_at: u64) -> Option<Vec<u8>> {
  let kind = StoredType::from_header(*bytes.first()?)?;
  match kind {
    StoredType::String => {
      let mut v = StringValue::deserialize(bytes).ok()?;
      v.expires_at = expires_at;
      Some(v.serialize())
    }
    StoredType::Hash => {
      let mut m = HashMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::List => {
      let mut m = ListMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::Set => {
      let mut m = SetMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::ZSet => {
      let mut m = ZSetMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::Bitmap => {
      let mut m = BitmapMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::BloomFilter => {
      let mut m = BloomFilterMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::HyperLogLog => {
      let mut m = HyperLogLogMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
    StoredType::Json => {
      let mut m = JsonMetadata::deserialize(bytes).ok()?;
      m.expires_at = expires_at;
      Some(m.serialize())
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::NO_EXPIRATION;

  fn header(ty: u8) -> u8 {
    (crate::encoding::CURRENT_VERSION << 4) | ty
  }

  #[test]
  fn test_stored_type_from_header() {
    assert_eq!(
      StoredType::from_header(header(TYPE_STRING)),
      Some(StoredType::String)
    );
    assert_eq!(
      StoredType::from_header(header(TYPE_HASH)),
      Some(StoredType::Hash)
    );
    assert_eq!(
      StoredType::from_header(header(TYPE_JSON)),
      Some(StoredType::Json)
    );
    assert_eq!(StoredType::from_header(0x00), None);
    assert_eq!(StoredType::from_header(0x0F), None);
  }

  #[test]
  fn test_stored_type_reply_names() {
    assert_eq!(StoredType::String.as_str(), "string");
    assert_eq!(StoredType::Hash.as_str(), "hash");
    assert_eq!(StoredType::List.as_str(), "list");
    assert_eq!(StoredType::Set.as_str(), "set");
    assert_eq!(StoredType::ZSet.as_str(), "zset");
    assert_eq!(StoredType::Bitmap.as_str(), "bitmap");
    assert_eq!(StoredType::Json.as_str(), "json");
  }

  /// The regression this module exists to prevent: `StringValue`'s fixed
  /// layout must never be read back as `HashMetadata` (which would lose the
  /// real `expires_at` and report a live string as "hash").
  #[test]
  fn test_string_expiry_is_not_lost() {
    let raw = StringValue::with_expiration(b"will_expire", 1_000).serialize();
    let meta = ValueMeta::decode(&raw).expect("decodes");
    assert_eq!(meta.kind, StoredType::String);
    assert_eq!(meta.expires_at, 1_000);
    assert!(is_expired(&raw, 2_000));
    assert!(!is_expired(&raw, 999));
  }

  #[test]
  fn test_string_without_expiration_never_expires() {
    let raw = StringValue::new(b"forever").serialize();
    assert_eq!(ValueMeta::decode(&raw).unwrap().expires_at, NO_EXPIRATION);
    assert!(!is_expired(&raw, u64::MAX));
  }

  #[test]
  fn test_complex_metadata_round_trip() {
    let mut hm = HashMetadata::new();
    hm.expires_at = 5_000;
    hm.version = 42;
    let raw = hm.serialize();

    let meta = ValueMeta::decode(&raw).expect("decodes");
    assert_eq!(meta.kind, StoredType::Hash);
    assert_eq!(meta.expires_at, 5_000);
    assert_eq!(meta.version, 42);
    assert!(meta.kind.is_complex());
    assert!(is_expired(&raw, 5_000));
    assert!(!is_expired(&raw, 4_999));
  }

  #[test]
  fn test_json_has_no_version() {
    let mut jm = JsonMetadata::new(b"{}".to_vec());
    jm.expires_at = 7_000;
    let raw = jm.serialize();

    let meta = ValueMeta::decode(&raw).expect("decodes");
    assert_eq!(meta.kind, StoredType::Json);
    assert_eq!(meta.expires_at, 7_000);
    assert_eq!(meta.version, 0);
    assert!(!meta.kind.is_complex());
  }

  #[test]
  fn test_with_expires_at_rewrites_only_expiry() {
    let raw = StringValue::new(b"payload").serialize();
    let updated = with_expires_at(&raw, 12_345).expect("re-encodes");

    let meta = ValueMeta::decode(&updated).unwrap();
    assert_eq!(meta.expires_at, 12_345);
    assert_eq!(StringValue::deserialize(&updated).unwrap().data, b"payload");
  }

  #[test]
  fn test_decode_rejects_empty_and_unknown() {
    assert!(ValueMeta::decode(&[]).is_none());
    assert!(ValueMeta::decode(&[0x0F, 0, 0]).is_none());
    assert!(!is_expired(&[], u64::MAX));
    assert!(!is_expired(&[0x0F, 0, 0], u64::MAX));
  }

  #[test]
  fn test_every_complex_type_round_trips() {
    use crate::encoding::{
      BitmapMetadata, BloomFilterMetadata, HyperLogLogMetadata, ListMetadata, SetMetadata,
      ZSetMetadata,
    };

    // Each metadata starts at expires_at with version = 7 so a wrong-type decode
    // or a shifted field would fail the assertions below.
    let cases: Vec<(StoredType, Vec<u8>)> = vec![
      (StoredType::Hash, {
        let mut m = HashMetadata::new();
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
      (StoredType::List, {
        let mut m = ListMetadata::new();
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
      (StoredType::Set, {
        let mut m = SetMetadata::new();
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
      (StoredType::ZSet, {
        let mut m = ZSetMetadata::new();
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
      (StoredType::Bitmap, {
        let mut m = BitmapMetadata::new();
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
      (StoredType::BloomFilter, {
        let mut m = BloomFilterMetadata::new(0.01, 100, 2);
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
      (StoredType::HyperLogLog, {
        let mut m = HyperLogLogMetadata::new();
        m.expires_at = 5_000;
        m.version = 7;
        m.serialize()
      }),
    ];

    for (expected_kind, raw) in cases {
      let meta = ValueMeta::decode(&raw).unwrap_or_else(|| panic!("{expected_kind:?} decodes"));
      assert_eq!(meta.kind, expected_kind);
      assert_eq!(meta.expires_at, 5_000, "{expected_kind:?} expires_at");
      assert_eq!(meta.version, 7, "{expected_kind:?} version");
      assert!(meta.kind.is_complex(), "{expected_kind:?} is complex");
      assert!(is_expired(&raw, 5_000));
      assert!(!is_expired(&raw, 4_999));
    }
  }
}
