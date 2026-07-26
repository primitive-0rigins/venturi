use std::env;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Read,
    Write,
    Admin,
}

impl Scope {
    pub fn allows(self, required: Self) -> bool {
        matches!(self, Self::Admin)
            || matches!(
                (self, required),
                (Self::Write, Self::Write) | (Self::Write, Self::Read) | (Self::Read, Self::Read)
            )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKey {
    /// Configured key name is the service principal. Never trust agent_id from a request body.
    pub name: String,
    pub value: String,
    pub scope: Scope,
    pub namespaces: Vec<String>,
}

impl ApiKey {
    pub fn grants_namespace(&self, namespace: &str) -> bool {
        self.namespaces
            .iter()
            .any(|grant| grant == "*" || grant == namespace)
    }
}

pub fn hipaa_profile() -> bool {
    matches!(
        env::var("VENTURI_DEPLOYMENT_PROFILE").as_deref(),
        Ok("hipaa" | "HIPAA")
    )
}

/// Decode the customer-managed Ed25519 seed used for signed audit exports.
/// It must be supplied as exactly 64 lowercase or uppercase hexadecimal digits.
pub fn audit_signing_key() -> Result<[u8; 32], String> {
    let value = env::var("VENTURI_AUDIT_SIGNING_KEY")
        .map_err(|_| "VENTURI_AUDIT_SIGNING_KEY is required".to_string())?;
    if value.len() != 64 {
        return Err("VENTURI_AUDIT_SIGNING_KEY must be a 32-byte hexadecimal seed".into());
    }
    let mut key = [0u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "VENTURI_AUDIT_SIGNING_KEY must be a 32-byte hexadecimal seed")?;
    }
    Ok(key)
}

/// `VENTURI_AGENT_KEYS=name:key:scope:namespaces`, where namespaces is a `|`-separated
/// allow-list (for example `ehr-writer:secret:write:clinical|billing`). The legacy
/// three-field form remains available outside the HIPAA profile and receives `*` with a
/// migration warning at process startup.
pub fn configured_keys() -> Vec<ApiKey> {
    let mut keys = Vec::new();
    if let Ok(value) = env::var("VENTURI_ADMIN_KEY") {
        if !value.trim().is_empty() {
            keys.push(ApiKey {
                name: "admin".into(),
                value,
                scope: Scope::Admin,
                namespaces: vec!["*".into()],
            });
        }
    }
    if let Ok(value) = env::var("VENTURI_AGENT_KEYS") {
        for entry in value.split(',') {
            let mut parts = entry.splitn(4, ':');
            let (Some(name), Some(value), Some(scope)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let scope = match scope {
                "read" => Scope::Read,
                "write" => Scope::Write,
                "admin" => Scope::Admin,
                _ => continue,
            };
            if name.trim().is_empty() || value.trim().is_empty() {
                continue;
            }
            let namespaces = parts.next().map(|v| {
                v.split('|')
                    .filter(|n| !n.trim().is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            });
            let Some(namespaces) =
                namespaces.or_else(|| (!hipaa_profile()).then(|| vec!["*".into()]))
            else {
                continue;
            };
            if namespaces.is_empty() {
                continue;
            }
            keys.push(ApiKey {
                name: name.to_string(),
                value: value.to_string(),
                scope,
                namespaces,
            });
        }
    }
    keys
}

pub fn required_scope(path: &str) -> Scope {
    if path == "/ingest" || path == "/verdict" {
        Scope::Write
    } else if path.starts_with("/retrieve/")
        || path.starts_with("/audit/")
        || path.starts_with("/chain/references/")
    {
        Scope::Read
    } else {
        Scope::Admin
    }
}

pub fn validate_hipaa_environment() -> Result<(), String> {
    if !hipaa_profile() {
        return Ok(());
    }
    if env::var("VENTURI_RETENTION_DAYS")
        .ok()
        .filter(|v| v == "indefinite" || v.parse::<u64>().is_ok_and(|n| n > 0))
        .is_none()
    {
        return Err("HIPAA profile requires VENTURI_RETENTION_DAYS to be a positive integer or `indefinite`".into());
    }
    if audit_signing_key().is_err() {
        return Err("HIPAA profile requires a valid VENTURI_AUDIT_SIGNING_KEY".into());
    }
    for name in [
        "VENTURI_TLS_PROXY",
        "VENTURI_UI_OIDC_ISSUER",
        "VENTURI_UI_OIDC_CLIENT_ID",
    ] {
        if env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .is_none()
        {
            return Err(format!("HIPAA profile requires {name}"));
        }
    }
    if env::var("VENTURI_AGENT_KEYS")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_none()
        || configured_keys().iter().all(|key| key.name == "admin")
    {
        return Err("HIPAA profile requires named VENTURI_AGENT_KEYS with namespace grants".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_grants_are_explicit() {
        let key = ApiKey {
            name: "svc".into(),
            value: "x".into(),
            scope: Scope::Write,
            namespaces: vec!["clinical".into()],
        };
        assert!(key.grants_namespace("clinical"));
        assert!(!key.grants_namespace("billing"));
    }
    #[test]
    fn scope_is_fail_closed() {
        assert_eq!(required_scope("/hold"), Scope::Admin);
    }
}
