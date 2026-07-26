use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Key, Nonce,
};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::types::error::TunnelError;

/// 32-byte symmetric key for a single gate.
pub type GateKey = [u8; 32];

/// Metadata stamped by a gate — used for pipeline tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateMeta {
    pub gate_id: u8,
    pub compression_level: i32,
    pub plaintext_size: u64,
    pub sealed_size: u64,
}

/// A gate in the wormhole pipeline.
///
/// Supports three operations:
///   seal()     — compress + encrypt (original combined gate, kept for compatibility)
///   compress() — zstd only, no encryption (Gate 2)
///   encrypt()  — ChaCha20-Poly1305 only, no compression (Gate 3)
///   unseal()   — decrypt + decompress (reverse of seal)
///   decrypt()  — ChaCha20-Poly1305 only, no decompression (Exit Gate 1)
#[derive(Debug, Clone)]
pub struct CompressionGate {
    pub gate_id: u8,
    pub compression_level: i32,
}

impl CompressionGate {
    pub fn new(gate_id: u8, compression_level: i32) -> Self {
        Self {
            gate_id,
            compression_level: compression_level.clamp(1, 22),
        }
    }

    /// Gate 2: compress only — zstd, no encryption.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, TunnelError> {
        zstd::encode_all(data, self.compression_level)
            .map_err(|e| TunnelError::CompressionFailed(e.to_string()))
    }

    /// Gate 3: encrypt only — ChaCha20-Poly1305, no compression.
    /// Returns (sealed_bytes, key). sealed_bytes layout: nonce (12) || ciphertext+tag.
    /// The raw key must be written to the exit gate keystore immediately by the caller.
    pub fn encrypt(&self, data: &[u8]) -> Result<(Vec<u8>, GateKey), TunnelError> {
        let mut rng = rand::rng();
        let mut key_bytes: GateKey = [0u8; 32];
        rng.fill_bytes(&mut key_bytes);
        let sealed = self.encrypt_with_key(data, &key_bytes)?;
        Ok((sealed, key_bytes))
    }

    /// Gate 3 with caller-supplied chain key.
    /// Used by Gatekeeper so every orb in a chain shares one custody key while
    /// still receiving an independent random nonce.
    pub fn encrypt_with_key(&self, data: &[u8], key: &GateKey) -> Result<Vec<u8>, TunnelError> {
        let mut rng = rand::rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);

        let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, data)
            .map_err(|_| TunnelError::EncryptionFailed)?;

        let mut sealed = nonce_bytes.to_vec();
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed)
    }

    /// Exit Gate 1: decrypt only — ChaCha20-Poly1305, no decompression.
    /// sealed layout: nonce (12 bytes) || ciphertext+tag.
    pub fn decrypt(&self, sealed: &[u8], key: &GateKey) -> Result<Vec<u8>, TunnelError> {
        if sealed.len() < 12 {
            return Err(TunnelError::OrbCorrupted {
                id: format!("gate-{}", self.gate_id),
            });
        }
        let (nonce_bytes, ciphertext) = sealed.split_at(12);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
        let nonce = Nonce::from_slice(nonce_bytes);
        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| TunnelError::WrongKey {
                gate_id: self.gate_id,
            })
    }

    /// Combined compress + encrypt. Kept for the original roundtrip tests.
    /// sealed_bytes layout: nonce (12 bytes) || ciphertext
    pub fn seal(&self, data: &[u8]) -> Result<(Vec<u8>, GateMeta, GateKey), TunnelError> {
        let compressed = self.compress(data)?;
        let (sealed, key) = self.encrypt(&compressed)?;

        let meta = GateMeta {
            gate_id: self.gate_id,
            compression_level: self.compression_level,
            plaintext_size: data.len() as u64,
            sealed_size: sealed.len() as u64,
        };
        Ok((sealed, meta, key))
    }

    /// Combined decrypt + decompress. Reverse of seal().
    pub fn unseal(&self, sealed: &[u8], key: &GateKey) -> Result<Vec<u8>, TunnelError> {
        let compressed = self.decrypt(sealed, key)?;
        zstd::decode_all(compressed.as_slice())
            .map_err(|e| TunnelError::RehydrationFailed(e.to_string()))
    }
}
