mod server;

use server::{build_router, lock_mutex, SharedVenturi};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use venturi::pipeline::sweep::SweepReport;
use venturi::{LifecycleConfig, StorageLimits, TunnelError, Venturi, VenturiConfig};

#[tokio::main]
async fn main() {
    if env::var("VENTURI_ADMIN_KEY")
        .map(|key| key.trim().is_empty())
        .unwrap_or(true)
    {
        eprintln!("VENTURI_ADMIN_KEY is required; refusing to start without authentication.");
        std::process::exit(2);
    }
    let port = env::var("VENTURI_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9271);

    let data_dir = env::var("VENTURI_DATA").unwrap_or_else(|_| {
        format!(
            "{}/venturi-data",
            env::var("HOME").unwrap_or_else(|_| "/tmp".into())
        )
    });

    let venturi = open_venturi(&data_dir);
    let shared: SharedVenturi = Arc::new(Mutex::new(venturi));

    spawn_sweeps(Arc::clone(&shared));

    let app = build_router(shared);
    let addr = format!("127.0.0.1:{}", port);
    println!("Venturi listening on http://{}", addr);
    println!("Sweeps: access_marks=5min  tiers=15min  lifecycle=60s  expiry=daily  embeddings=30s  communities=30min");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");
    axum::serve(listener, app).await.expect("server error");
}

fn open_venturi(data_dir: &str) -> Venturi {
    let keys_dir = format!("{}/keys", data_dir);
    std::fs::create_dir_all(&keys_dir).expect("failed to create keys dir");
    std::fs::create_dir_all(format!("{}/shelf", data_dir)).expect("failed to create shelf dir");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&keys_dir, std::fs::Permissions::from_mode(0o700));
    }

    let ollama_url = env::var("VENTURI_OLLAMA").unwrap_or_else(|_| "http://localhost:11434".into());
    let embedding_model = env::var("VENTURI_EMBEDDING_MODEL").ok();
    let embedding_dim = env::var("VENTURI_EMBEDDING_DIM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());

    Venturi::open(VenturiConfig {
        shelf_root: format!("{}/shelf", data_dir),
        journal_db: format!("{}/journal.db", data_dir),
        keystore_db: format!("{}/keys/keystore.db", data_dir),
        librarian_db: format!("{}/librarian.db", data_dir),
        scribe_db: format!("{}/scribe.db", data_dir),
        graph_db: format!("{}/graph.db", data_dir),
        ollama_url,
        embedding_model,
        embedding_dim,
        lifecycle: Some(LifecycleConfig::default()),
        limits: StorageLimits::default(),
    })
    .expect("failed to open Venturi")
}

/// Spawn the background maintenance tasks.
/// Each shares the same Arc — sweep timers are offset so they don't
/// all fire at once after a restart.
fn spawn_sweeps(shared: SharedVenturi) {
    // Sweep 1: sibling refresh — every 5 minutes
    spawn_skipped_interval(
        Arc::clone(&shared),
        "access_marks",
        Duration::from_secs(5 * 60),
        false,
        |v| v.sweep_access_marks(),
    );

    // Sweep 2: tier update — every 15 minutes
    spawn_skipped_interval(
        Arc::clone(&shared),
        "tiers",
        Duration::from_secs(15 * 60),
        false,
        |v| v.sweep_tiers(),
    );

    // Sweep 3: 90-day expiry — once daily
    spawn_skipped_interval(
        Arc::clone(&shared),
        "expiry",
        Duration::from_secs(24 * 60 * 60),
        false,
        |v| v.sweep_expiry(),
    );

    // Sweep 4: lifecycle manager — every 60 seconds
    spawn_skipped_interval(
        Arc::clone(&shared),
        "lifecycle",
        Duration::from_secs(60),
        true,
        |v| v.lifecycle_sweep(),
    );

    // Sweep 5: spectral community detection — every 30 minutes
    spawn_skipped_interval(
        Arc::clone(&shared),
        "communities",
        Duration::from_secs(30 * 60),
        false,
        |v| v.sweep_communities(),
    );

    // Sweep 6: embedding queue — every 30 seconds, no initial skip
    // Processes leftover queue items immediately on startup, then on interval.
    let v = Arc::clone(&shared);
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(30));
        loop {
            t.tick().await;
            let v2 = Arc::clone(&v);
            let _ = tokio::task::spawn_blocking(move || lock_mutex(&v2).process_embedding_queue())
                .await;
        }
    });
}

fn spawn_skipped_interval<F>(
    shared: SharedVenturi,
    name: &'static str,
    interval: Duration,
    disable_after_failures: bool,
    sweep: F,
) where
    F: Fn(&Venturi) -> Result<SweepReport, TunnelError> + Send + Sync + Copy + 'static,
{
    tokio::spawn(async move {
        let mut t = tokio::time::interval(interval);
        let mut failures = 0u8;
        let mut disabled = false;
        t.tick().await;
        loop {
            t.tick().await;
            if disabled {
                continue;
            }
            let v = Arc::clone(&shared);
            let outcome = tokio::task::spawn_blocking(move || {
                let guard = lock_mutex(&v);
                let result = sweep(&guard);
                record_sweep_health(&guard, name, &result, failures);
                result
            })
            .await;

            match outcome {
                Ok(Ok(_)) => failures = 0,
                Ok(Err(_)) | Err(_) => failures = failures.saturating_add(1),
            }
            if disable_after_failures && failures >= 3 {
                disabled = true;
                let guard = lock_mutex(&shared);
                let _ = guard.record_daemon_health(name, "disabled", failures, None);
            }
        }
    });
}

fn record_sweep_health(
    venturi: &Venturi,
    name: &str,
    result: &Result<SweepReport, TunnelError>,
    prior_failures: u8,
) {
    match result {
        Ok(report) => {
            let details = format!(
                "chains_affected={} orbs_ejected={}",
                report.chains_affected, report.orbs_ejected
            );
            let _ = venturi.record_daemon_health(name, "ok", 0, Some(&details));
        }
        Err(error) => {
            let failures = prior_failures.saturating_add(1);
            let details = error.to_string();
            let _ = venturi.record_daemon_health(name, "error", failures, Some(&details));
        }
    }
}
