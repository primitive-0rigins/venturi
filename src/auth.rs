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

#[derive(Clone)]
pub struct ApiKey {
    pub value: String,
    pub scope: Scope,
}

pub fn configured_keys() -> Vec<ApiKey> {
    let mut keys = Vec::new();
    if let Ok(value) = env::var("VENTURI_ADMIN_KEY") {
        if !value.trim().is_empty() {
            keys.push(ApiKey {
                value,
                scope: Scope::Admin,
            });
        }
    }
    if let Ok(value) = env::var("VENTURI_AGENT_KEYS") {
        for entry in value.split(',') {
            let mut parts = entry.splitn(3, ':');
            let _name = parts.next();
            let key = parts.next();
            let scope = parts.next();
            let scope = match scope {
                Some("read") => Scope::Read,
                Some("write") => Scope::Write,
                Some("admin") => Scope::Admin,
                _ => continue,
            };
            if let Some(value) = key.filter(|key| !key.trim().is_empty()) {
                keys.push(ApiKey {
                    value: value.to_string(),
                    scope,
                });
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // configured_keys() reads process-wide env vars; serialize any test that
    // touches them so parallel `cargo test` runs don't clobber each other.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap();
        let previous: Vec<(&str, Option<String>)> =
            vars.iter().map(|(k, _)| (*k, env::var(k).ok())).collect();
        for (k, v) in vars {
            unsafe {
                match v {
                    Some(value) => env::set_var(k, value),
                    None => env::remove_var(k),
                }
            }
        }
        f();
        for (k, v) in previous {
            unsafe {
                match v {
                    Some(value) => env::set_var(k, value),
                    None => env::remove_var(k),
                }
            }
        }
    }

    #[test]
    fn admin_scope_allows_everything() {
        assert!(Scope::Admin.allows(Scope::Admin));
        assert!(Scope::Admin.allows(Scope::Write));
        assert!(Scope::Admin.allows(Scope::Read));
    }

    #[test]
    fn write_scope_allows_write_and_read_not_admin() {
        assert!(Scope::Write.allows(Scope::Write));
        assert!(Scope::Write.allows(Scope::Read));
        assert!(!Scope::Write.allows(Scope::Admin));
    }

    #[test]
    fn read_scope_allows_only_read() {
        assert!(Scope::Read.allows(Scope::Read));
        assert!(!Scope::Read.allows(Scope::Write));
        assert!(!Scope::Read.allows(Scope::Admin));
    }

    #[test]
    fn required_scope_maps_known_paths() {
        assert_eq!(required_scope("/ingest"), Scope::Write);
        assert_eq!(required_scope("/verdict"), Scope::Write);
        assert_eq!(required_scope("/retrieve/context"), Scope::Read);
        assert_eq!(required_scope("/audit/abc"), Scope::Read);
        assert_eq!(required_scope("/chain/references/abc"), Scope::Read);
    }

    #[test]
    fn required_scope_defaults_unlisted_paths_to_admin() {
        // Fail closed: anything not explicitly classified as read/write
        // (including /hold and /chain/link) requires the admin key.
        assert_eq!(required_scope("/hold"), Scope::Admin);
        assert_eq!(required_scope("/hold/abc"), Scope::Admin);
        assert_eq!(required_scope("/chain/link"), Scope::Admin);
        assert_eq!(required_scope("/unknown"), Scope::Admin);
    }

    #[test]
    fn configured_keys_reads_admin_key_from_env() {
        with_env(
            &[
                ("VENTURI_ADMIN_KEY", Some("admin-secret")),
                ("VENTURI_AGENT_KEYS", None),
            ],
            || {
                let keys = configured_keys();
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].value, "admin-secret");
                assert_eq!(keys[0].scope, Scope::Admin);
            },
        );
    }

    #[test]
    fn configured_keys_ignores_blank_admin_key() {
        with_env(
            &[
                ("VENTURI_ADMIN_KEY", Some("   ")),
                ("VENTURI_AGENT_KEYS", None),
            ],
            || {
                assert!(configured_keys().is_empty());
            },
        );
    }

    #[test]
    fn configured_keys_parses_agent_keys_by_scope() {
        with_env(
            &[
                ("VENTURI_ADMIN_KEY", None),
                (
                    "VENTURI_AGENT_KEYS",
                    Some("reader:key-r:read,writer:key-w:write,root:key-a:admin"),
                ),
            ],
            || {
                let keys = configured_keys();
                assert_eq!(keys.len(), 3);
                assert!(keys
                    .iter()
                    .any(|k| k.value == "key-r" && k.scope == Scope::Read));
                assert!(keys
                    .iter()
                    .any(|k| k.value == "key-w" && k.scope == Scope::Write));
                assert!(keys
                    .iter()
                    .any(|k| k.value == "key-a" && k.scope == Scope::Admin));
            },
        );
    }

    #[test]
    fn configured_keys_skips_malformed_agent_entries() {
        with_env(
            &[
                ("VENTURI_ADMIN_KEY", None),
                // missing scope, and an entry with an empty key value
                ("VENTURI_AGENT_KEYS", Some("bad:key-only-name,ok::read")),
            ],
            || {
                assert!(configured_keys().is_empty());
            },
        );
    }
}
