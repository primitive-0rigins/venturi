use crate::types::error::TunnelError;

/// Restrict a SQLite database and its WAL sidecars to the owning user.
pub(crate) fn restrict_database_files(path: &str) -> Result<(), TunnelError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let permissions = std::fs::Permissions::from_mode(0o600);
        for candidate in [
            path.to_string(),
            format!("{path}-wal"),
            format!("{path}-shm"),
        ] {
            if std::path::Path::new(&candidate).exists() {
                std::fs::set_permissions(candidate, permissions.clone())?;
            }
        }
    }

    Ok(())
}
