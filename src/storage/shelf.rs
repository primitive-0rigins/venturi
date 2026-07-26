use crate::types::{
    error::TunnelError,
    orb::{Orb, OrbId},
};
use std::path::{Path, PathBuf};

/// Orb storage on the 4TB external drive.
///
/// Files are addressed by OrbId hex with two-level directory sharding:
///   {root}/{orb_id[0..2]}/{orb_id[2..4]}/{orb_id}
///
/// This prevents filesystem bottlenecks with millions of files in one directory.
/// Writes are atomic: temp file → rename. No bincode — fixed binary format.
pub struct OrbShelf {
    root: PathBuf,
}

impl OrbShelf {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Store an orb. Atomic write — crash-safe.
    pub fn store(&self, orb: &Orb) -> Result<(), TunnelError> {
        let path = self.orb_path(&orb.id);
        std::fs::create_dir_all(path.parent().unwrap())?;
        restrict_directory(path.parent().unwrap())?;
        let bytes = orb.to_bytes();
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        restrict_file(&tmp)?;
        std::fs::rename(&tmp, &path)?;
        restrict_file(&path)?;
        Ok(())
    }

    /// Load an orb by its deterministic address.
    /// Integrity verified inside Orb::from_bytes via OrbId recomputation.
    pub fn load(&self, id: &OrbId, parent_id: String) -> Result<Orb, TunnelError> {
        let path = self.orb_path(id);
        let bytes =
            std::fs::read(&path).map_err(|_| TunnelError::OrbNotFound { id: id.to_string() })?;
        Orb::from_bytes(&bytes, parent_id).map_err(|e| TunnelError::OrbCorrupted {
            id: format!("{}: {}", id, e),
        })
    }

    /// Remove an orb by hex string. Used during rollback cleanup.
    /// Returns Ok(()) even if the file didn't exist.
    pub fn remove(&self, orb_id_hex: &str) -> Result<(), TunnelError> {
        if orb_id_hex.len() < 4 {
            return Ok(());
        }
        let path = self
            .root
            .join(&orb_id_hex[0..2])
            .join(&orb_id_hex[2..4])
            .join(orb_id_hex);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    pub fn exists(&self, id: &OrbId) -> bool {
        self.orb_path(id).exists()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Two-level sharded path: {root}/{id[0..2]}/{id[2..4]}/{id}
    pub fn orb_path(&self, id: &OrbId) -> PathBuf {
        let hex = id.as_hex();
        self.root.join(&hex[0..2]).join(&hex[2..4]).join(&hex)
    }
}

fn restrict_directory(path: &Path) -> Result<(), TunnelError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), TunnelError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
