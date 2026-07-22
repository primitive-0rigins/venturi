use crate::gate::CompressionGate;
use crate::types::error::TunnelError;
use crate::types::orb::Orb;

/// Exit gate pipeline — reverse of the wormhole ingestion path.
///
/// Rehydrates sealed orbs back to original content in transit:
///   Exit Gate 1 (Unlock):     ChaCha20-Poly1305 decrypt using chain key from keystore
///   Exit Gate 2 (Decompress): zstd decompress
///
/// For document mode, all chain members are unlocked in parallel before
/// this pipeline runs on each. Assembly into a full document (sort by sequence,
/// concatenate) happens after all orbs have been individually rehydrated.
pub struct RetrievalPipeline {
    gate3_decrypt: CompressionGate,
}

impl RetrievalPipeline {
    pub fn new() -> Self {
        Self {
            gate3_decrypt: CompressionGate::new(3, 1),
        }
    }

    /// Rehydrate a single orb back to original content bytes.
    ///
    /// chain_key: raw ChaCha20 key fetched from the exit gate keystore.
    /// Decryption is authenticated — wrong key or corrupted content returns an error.
    pub fn rehydrate(&self, orb: Orb, chain_key: &[u8; 32]) -> Result<Vec<u8>, TunnelError> {
        let sealed = orb.sealed_bytes();
        let parent_id = orb.parent_id.clone();
        let sequence = orb.sequence;

        // Exit Gate 1: decrypt (ChaCha20-Poly1305)
        let compressed = self
            .gate3_decrypt
            .decrypt(&sealed, chain_key)
            .map_err(|_| TunnelError::ChainAssemblyFailed {
                parent_id,
                sequence,
            })?;

        // Exit Gate 2: decompress (zstd)
        zstd::decode_all(compressed.as_slice())
            .map_err(|e| TunnelError::RehydrationFailed(e.to_string()))
    }
}

impl Default for RetrievalPipeline {
    fn default() -> Self {
        Self::new()
    }
}
