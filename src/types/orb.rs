use sha2::{Digest, Sha256};
use std::fmt;

/// Deterministic address of a stored orb.
/// SHA256 of the sealed bytes (nonce || encrypted content) computed post-Gate3.
/// Random nonce means same content encrypted twice = different OrbIds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrbId(pub [u8; 32]);

impl OrbId {
    /// Derive OrbId from sealed bytes (nonce || ciphertext+tag).
    /// Always called after Gate3 encryption — never before.
    pub fn from_sealed(sealed: &[u8]) -> Self {
        Self(Sha256::digest(sealed).into())
    }

    pub fn as_hex(&self) -> String {
        self.0.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Reconstruct an OrbId from a 64-character hex string.
    /// Returns None if the string is not exactly 64 hex characters.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0])?;
            let lo = hex_nibble(chunk[1])?;
            bytes[i] = (hi << 4) | lo;
        }
        Some(Self(bytes))
    }
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

impl fmt::Display for OrbId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.as_hex()[..16])
    }
}

/// Magic bytes for the orb binary file format: "VERT"
pub const ORB_MAGIC: [u8; 4] = [0x56, 0x45, 0x52, 0x54];
pub const ORB_VERSION: u32 = 1;

/// A sealed, encrypted, compressed unit of content.
///
/// Stores full original content — never a summary or fragment.
/// Cannot self-decrypt. Requires the chain key from the exit gate keystore.
///
/// Binary format on disk:
///   [4]  magic: "VERT"
///   [4]  version: u32 le
///   [32] orb_id bytes
///   [32] parent_id_hash: SHA256 of parent_id string
///   [4]  sequence: u32 le  (1-based)
///   [4]  chain_length: u32 le
///   [12] nonce: ChaCha20 nonce
///   [4]  content_length: u32 le
///   [N]  content: encrypted+compressed bytes (includes embedded Poly1305 tag)
#[derive(Debug, Clone)]
pub struct Orb {
    pub id: OrbId,
    /// Chain identifier — UUID v4. Self-referencing for single-orb ingestions.
    pub parent_id: String,
    /// 1-based position in chain.
    pub sequence: u32,
    /// Total orbs in this chain.
    pub chain_length: u32,
    /// ChaCha20 nonce (12 bytes) used during Gate3 encryption.
    pub nonce: [u8; 12],
    /// Encrypted+compressed content including embedded Poly1305 auth tag.
    pub content: Vec<u8>,
}

impl Orb {
    /// Create an orb from Gate3 sealed bytes (nonce || ciphertext+tag).
    /// OrbId is computed from the full sealed bytes.
    pub fn from_sealed(
        sealed: Vec<u8>,
        parent_id: String,
        sequence: u32,
        chain_length: u32,
    ) -> Self {
        let id = OrbId::from_sealed(&sealed);
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&sealed[..12]);
        let content = sealed[12..].to_vec();
        Self {
            id,
            parent_id,
            sequence,
            chain_length,
            nonce,
            content,
        }
    }

    /// Reassemble sealed bytes (nonce || content) for decryption by exit gate.
    pub fn sealed_bytes(&self) -> Vec<u8> {
        let mut v = self.nonce.to_vec();
        v.extend_from_slice(&self.content);
        v
    }

    pub fn content_size(&self) -> usize {
        self.content.len()
    }

    /// Serialize to the fixed binary format for disk storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        let parent_id_hash: [u8; 32] = Sha256::digest(self.parent_id.as_bytes()).into();
        let content_len = self.content.len() as u32;

        let mut buf = Vec::with_capacity(96 + self.content.len());
        buf.extend_from_slice(&ORB_MAGIC);
        buf.extend_from_slice(&ORB_VERSION.to_le_bytes());
        buf.extend_from_slice(self.id.as_bytes());
        buf.extend_from_slice(&parent_id_hash);
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.extend_from_slice(&self.chain_length.to_le_bytes());
        buf.extend_from_slice(&self.nonce);
        buf.extend_from_slice(&content_len.to_le_bytes());
        buf.extend_from_slice(&self.content);
        buf
    }

    /// Parse from the fixed binary format. Verifies magic and OrbId integrity.
    pub fn from_bytes(bytes: &[u8], parent_id: String) -> Result<Self, String> {
        const MIN_LEN: usize = 4 + 4 + 32 + 32 + 4 + 4 + 12 + 4; // 96 bytes header
        if bytes.len() < MIN_LEN {
            return Err("orb too short".into());
        }
        if bytes[0..4] != ORB_MAGIC {
            return Err("invalid magic bytes".into());
        }

        let version = u32::from_le_bytes(
            bytes[4..8]
                .try_into()
                .map_err(|_| "orb version is truncated")?,
        );
        if version != ORB_VERSION {
            return Err(format!("unsupported orb version: {version}"));
        }

        let mut pos = 8; // skip magic(4) + version(4)
        let orb_id_bytes: [u8; 32] = bytes[pos..pos + 32]
            .try_into()
            .map_err(|_| "orb id is truncated")?;
        pos += 32;
        let stored_parent_id_hash: [u8; 32] = bytes[pos..pos + 32]
            .try_into()
            .map_err(|_| "orb parent binding is truncated")?;
        let expected_parent_id_hash: [u8; 32] = Sha256::digest(parent_id.as_bytes()).into();
        if stored_parent_id_hash != expected_parent_id_hash {
            return Err("orb parent_id binding mismatch".into());
        }
        pos += 32;
        let sequence = u32::from_le_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| "orb sequence is truncated")?,
        );
        pos += 4;
        let chain_length = u32::from_le_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| "orb chain length is truncated")?,
        );
        if sequence == 0 || chain_length == 0 || sequence > chain_length {
            return Err("invalid orb chain position".into());
        }
        pos += 4;
        let nonce: [u8; 12] = bytes[pos..pos + 12]
            .try_into()
            .map_err(|_| "orb nonce is truncated")?;
        pos += 12;
        let content_len = u32::from_le_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| "orb content length is truncated")?,
        ) as usize;
        pos += 4;

        let content_end = pos
            .checked_add(content_len)
            .ok_or_else(|| "orb content length overflow".to_owned())?;
        if bytes.len() < content_end {
            return Err("orb content truncated".into());
        }
        if bytes.len() != content_end {
            return Err("orb has trailing bytes".into());
        }
        let content = bytes[pos..content_end].to_vec();

        // Integrity check: recompute OrbId and compare
        let mut sealed = nonce.to_vec();
        sealed.extend_from_slice(&content);
        let expected_id = OrbId::from_sealed(&sealed);
        if expected_id.0 != orb_id_bytes {
            return Err("orb_id integrity check failed — content corrupted".into());
        }

        Ok(Self {
            id: OrbId(orb_id_bytes),
            parent_id,
            sequence,
            chain_length,
            nonce,
            content,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_orb() -> Orb {
        let mut sealed = vec![7_u8; 12];
        sealed.extend_from_slice(b"authenticated ciphertext");
        Orb::from_sealed(sealed, "parent-a".to_owned(), 1, 1)
    }

    fn parse_error(bytes: &[u8], parent_id: &str) -> String {
        match Orb::from_bytes(bytes, parent_id.to_owned()) {
            Ok(_) => "unexpected success".to_owned(),
            Err(error) => error,
        }
    }

    #[test]
    fn parser_rejects_unknown_version() {
        let mut bytes = sample_orb().to_bytes();
        bytes[4..8].copy_from_slice(&2_u32.to_le_bytes());

        assert_eq!(
            parse_error(&bytes, "parent-a"),
            "unsupported orb version: 2"
        );
    }

    #[test]
    fn parser_rejects_wrong_parent_binding() {
        let bytes = sample_orb().to_bytes();

        assert_eq!(
            parse_error(&bytes, "parent-b"),
            "orb parent_id binding mismatch"
        );
    }

    #[test]
    fn parser_rejects_trailing_bytes_and_invalid_chain_position() {
        let mut trailing = sample_orb().to_bytes();
        trailing.push(0);
        assert_eq!(parse_error(&trailing, "parent-a"), "orb has trailing bytes");

        let mut invalid_position = sample_orb().to_bytes();
        invalid_position[72..76].copy_from_slice(&2_u32.to_le_bytes());
        assert_eq!(
            parse_error(&invalid_position, "parent-a"),
            "invalid orb chain position"
        );
    }
}
