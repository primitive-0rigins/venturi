use crate::gate::CompressionGate;
use crate::types::error::TunnelError;
use crate::types::orb::Orb;
use rand::RngCore;

/// The wormhole ingestion pipeline.
///
/// Processes a content chunk through two gates in transit:
///   Gate 2 (Compress): zstd only
///   Gate 3 (Encrypt):  ChaCha20-Poly1305 only
///
/// Chunking (Gate 1) and chain tethering (Gate 4) are handled by the
/// Gatekeeper — not inside the wormhole itself.
///
/// The raw encryption key is returned to the caller for immediate deposit
/// into the exit gate keystore. The key must never be stored alongside the orb.
pub struct WormholeTunnel {
    gate2_compress: CompressionGate,
    gate3_encrypt: CompressionGate,
}

impl WormholeTunnel {
    pub fn new() -> Self {
        Self {
            gate2_compress: CompressionGate::new(2, 3),
            gate3_encrypt: CompressionGate::new(3, 1),
        }
    }

    /// Seal one chunk through the wormhole.
    ///
    /// Returns the sealed Orb and the raw chain key.
    /// Caller must write the key to the exit gate keystore immediately.
    ///
    /// parent_id:    UUID v4 chain identifier (from Gatekeeper)
    /// sequence:     1-based position of this chunk in the chain
    /// chain_length: total chunks in this ingestion
    pub fn seal_chunk(
        &self,
        data: Vec<u8>,
        parent_id: String,
        sequence: u32,
        chain_length: u32,
    ) -> Result<(Orb, [u8; 32]), TunnelError> {
        // Gate 2: compress only
        let compressed = self.gate2_compress.compress(&data)?;

        // Gate 3: encrypt only — raw key returned for keystore deposit
        let (sealed, key) = self.gate3_encrypt.encrypt(&compressed)?;

        let orb = Orb::from_sealed(sealed, parent_id, sequence, chain_length);
        Ok((orb, key))
    }

    /// Generate the single raw key for an ingestion chain.
    pub fn generate_chain_key(&self) -> [u8; 32] {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }

    /// Seal one chunk using the caller-supplied chain key.
    pub fn seal_chunk_with_key(
        &self,
        data: Vec<u8>,
        parent_id: String,
        sequence: u32,
        chain_length: u32,
        chain_key: &[u8; 32],
    ) -> Result<Orb, TunnelError> {
        let compressed = self.gate2_compress.compress(&data)?;
        let sealed = self
            .gate3_encrypt
            .encrypt_with_key(&compressed, chain_key)?;
        Ok(Orb::from_sealed(sealed, parent_id, sequence, chain_length))
    }
}

impl Default for WormholeTunnel {
    fn default() -> Self {
        Self::new()
    }
}
