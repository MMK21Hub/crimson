use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use native_tls::TlsConnector;
use postgres::{Client, config::SslMode};
use postgres_native_tls::MakeTlsConnector;
use reqwest::Url;
use rusqlite::Connection;

#[derive(Parser)]
struct CrimsonArgs {
    /// Path to the SQLite database file
    #[arg(long, default_value = "data/crimson.db")]
    database_file: std::path::PathBuf,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Payout(PayoutArgs),
}

#[derive(Args)]
struct PayoutArgs {
    /// Start time (ISO 6801, e.g. 2026-02-01T00:00:00Z)
    #[arg(long)]
    start: Option<String>,

    /// End time (ISO 6801, e.g. 2026-03-01T00:00:00Z). Defaults to now.
    #[arg(long)]
    end: Option<String>,

    /// Pays out helpers at a fixed rate of X cookies per ticket
    #[clap(long)]
    cookie_rate: Option<f64>,
}

struct PayoutSession {
    id: u64,
    period_start: time::OffsetDateTime,
    period_end: time::OffsetDateTime,
    completed_at: time::OffsetDateTime,
}

struct PayoutTransaction {
    id: u64,
    session: u64,
    user: u64,
    /// Amount of tickets (stardust, etc) given to the user
    amount: f64,
}

struct User {
    id: u64,
    stardance_id: u64,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS payout_session (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    completed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stardance_id INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS payout_transaction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session INTEGER NOT NULL REFERENCES payout_session(id),
    user INTEGER NOT NULL REFERENCES user(id),
    amount REAL NOT NULL
);
"#;

fn get_db(path: &Path) -> anyhow::Result<Connection> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create database directory {}", parent.display())
            })?;
        }
    }

    let conn = Connection::open(path)
        .with_context(|| format!("Failed to open database {}", path.display()))?;

    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", true)?;
    conn.execute_batch(SCHEMA)?;

    Ok(conn)
}

fn main() -> anyhow::Result<()> {
    // Configuration
    dotenvy::dotenv().ok();
    let nephthys_db_url =
        std::env::var("NEPHTHYS_DB_URL").context("NEPHTHYS_DB_URL environment variable not set")?;
    let flavortown_api = std::env::var("FLAVORTOWN_API_BASE")
        .context("FLAVORTOWN_API_BASE environment variable not set")?;
    let flavortown_api =
        Url::parse(&flavortown_api).context("FLAVORTOWN_API_BASE is not a valid URL")?;
    if flavortown_api.path().trim_end_matches("/") != "/api/v1" {
        println!(
            "Warning: FLAVORTOWN_API_BASE does not end in `/api/v1`. Are you sure you have the full URL?"
        );
    }
    let flavortown_api_key = std::env::var("FLAVORTOWN_API_KEY")
        .context("FLAVORTOWN_API_KEY environment variable not set")?;
    let args = CrimsonArgs::parse();
    let nephthys_db = if let Ok(connector) = TlsConnector::new() {
        let tls = MakeTlsConnector::new(connector);
        postgres::Client::connect(&nephthys_db_url, tls)
    } else {
        println!(
            "Warning: Failed to initialise TLS (no native TLS lib available?), falling back to unencrypted connection."
        );
        postgres::Client::connect(&nephthys_db_url, postgres::NoTls)
    };
    let db = get_db(&args.database_file)?;

    Ok(())
}
