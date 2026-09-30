//! Experimental aggregate dependency wire format (version 1).

use std::io::{self, Read, Write};

use super::WtxId;
use crate::serialization::{SerializationError, ZcashDeserialize, ZcashSerialize};

/// Maximum number of originals in a flat manifest.
pub const MAX_ORIGINALS: usize = 32;
/// Maximum wire payload size of a manifest response.
pub const MAX_MANIFEST_BYTES: usize = 67 + 64 * MAX_ORIGINALS;

/// A manifest bound to an aggregate's exact witnessed ID; empty means unavailable/refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    /// The exact aggregate authorization form.
    pub aggregate: WtxId,
    /// Canonically ordered originals, including the original carrier.
    pub originals: Vec<WtxId>,
}

#[cfg(any(test, feature = "proptest-impl"))]
impl proptest::arbitrary::Arbitrary for Manifest {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;
    fn arbitrary_with(_: ()) -> Self::Strategy {
        use proptest::prelude::*;
        (
            any::<WtxId>(),
            proptest::collection::vec(any::<WtxId>(), 0..=MAX_ORIGINALS),
        )
            .prop_map(|(aggregate, mut originals)| {
                originals.retain(|id| *id != aggregate);
                originals.sort_by_key(WtxId::as_bytes);
                originals.dedup_by_key(|id| id.id);
                if originals.len() == 1 {
                    originals.clear();
                }
                Self {
                    aggregate,
                    originals,
                }
            })
            .boxed()
    }
}

impl Manifest {
    /// Reject oversized, duplicate, noncanonical, or self-referential manifests.
    pub fn validate(&self) -> Result<(), SerializationError> {
        if !self.originals.is_empty() && !(2..=MAX_ORIGINALS).contains(&self.originals.len()) {
            return Err(SerializationError::Parse(
                "invalid aggregate dependency count",
            ));
        }
        if self
            .originals
            .windows(2)
            .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
            || self.originals.contains(&self.aggregate)
        {
            return Err(SerializationError::Parse("noncanonical aggregate manifest"));
        }
        let ids: std::collections::HashSet<_> = self.originals.iter().map(|id| id.id).collect();
        if ids.len() != self.originals.len() {
            return Err(SerializationError::Parse(
                "duplicate original transaction effects",
            ));
        }
        Ok(())
    }
}

/// Encode a versioned aggregate dependency request.
pub fn write_request(id: WtxId, mut writer: impl Write) -> io::Result<()> {
    writer.write_all(&[1])?;
    id.zcash_serialize(writer)
}

/// Decode a versioned aggregate dependency request.
pub fn read_request(mut reader: impl Read) -> Result<WtxId, SerializationError> {
    let mut version = [0];
    reader.read_exact(&mut version)?;
    if version != [1] {
        return Err(SerializationError::Parse(
            "unsupported aggregation protocol version",
        ));
    }
    WtxId::zcash_deserialize(reader)
}

impl ZcashSerialize for Manifest {
    fn zcash_serialize<W: Write>(&self, mut writer: W) -> io::Result<()> {
        self.validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        write_request(self.aggregate, &mut writer)?;
        let count = u16::try_from(self.originals.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "manifest too large"))?;
        writer.write_all(&count.to_le_bytes())?;
        for id in &self.originals {
            id.zcash_serialize(&mut writer)?;
        }
        Ok(())
    }
}

impl ZcashDeserialize for Manifest {
    fn zcash_deserialize<R: Read>(mut reader: R) -> Result<Self, SerializationError> {
        let aggregate = read_request(&mut reader)?;
        let mut count = [0; 2];
        reader.read_exact(&mut count)?;
        let count = usize::from(u16::from_le_bytes(count));
        // Bound allocation before reading any attacker-controlled list.
        if count == 1 || count > MAX_ORIGINALS {
            return Err(SerializationError::Parse(
                "invalid aggregate dependency count",
            ));
        }
        let mut originals = Vec::with_capacity(count);
        for _ in 0..count {
            originals.push(WtxId::zcash_deserialize(&mut reader)?);
        }
        let manifest = Self {
            aggregate,
            originals,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        Manifest {
            aggregate: [3; 64].into(),
            originals: vec![[1; 64].into(), [2; 64].into()],
        }
    }

    #[test]
    fn aggregate_manifest_wire_roundtrip_and_exact_layout() {
        let original = manifest();
        let bytes = original.zcash_serialize_to_vec().unwrap();
        assert_eq!(bytes.len(), 67 + 128);
        assert_eq!(bytes[0], 1);
        assert_eq!(&bytes[1..65], &[3; 64]);
        assert_eq!(&bytes[65..67], &2u16.to_le_bytes());
        assert_eq!(
            Manifest::zcash_deserialize(bytes.as_slice()).unwrap(),
            original
        );
        let empty = Manifest {
            originals: vec![],
            ..original
        };
        assert_eq!(empty.zcash_serialize_to_vec().unwrap().len(), 67);
    }

    #[test]
    fn aggregate_manifest_rejects_malformed_counts_and_versions_before_allocating() {
        let mut bytes = vec![1];
        bytes.extend([3; 64]);
        for count in [1u16, 33, u16::MAX] {
            let mut invalid = bytes.clone();
            invalid.extend(count.to_le_bytes());
            assert!(matches!(
                Manifest::zcash_deserialize(invalid.as_slice()),
                Err(SerializationError::Parse(_))
            ));
        }
        bytes[0] = 2;
        bytes.extend(0u16.to_le_bytes());
        assert!(Manifest::zcash_deserialize(bytes.as_slice()).is_err());
    }

    #[test]
    fn aggregate_manifest_rejects_duplicate_self_reference_and_order() {
        let valid = manifest();
        for originals in [
            vec![valid.originals[0], valid.originals[0]],
            vec![valid.originals[1], valid.originals[0]],
            vec![valid.originals[0], valid.aggregate],
            vec![
                valid.originals[0],
                WtxId {
                    id: valid.originals[0].id,
                    auth_digest: valid.originals[1].auth_digest,
                },
            ],
        ] {
            assert!(Manifest {
                originals,
                ..valid.clone()
            }
            .validate()
            .is_err());
        }
    }
}
