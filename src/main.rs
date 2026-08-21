use std::time::Duration;
use std::{collections::HashMap, path::Path};

use anyhow::{Context, anyhow};
use clap::{Args, Parser, Subcommand};
use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use reqwest::Url;
use rusqlite::Connection;
use serde::Deserialize;
use time::OffsetDateTime;

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

#[derive(Debug)]
struct User {
    id: u64,
    stardance_id: u64,
    slack_id: String,
}

#[derive(Deserialize, Debug)]
struct StardanceUser {
    id: i64,
    slack_id: String,
}

#[derive(Deserialize, Debug)]
struct StardanceUsersResponse {
    users: Vec<StardanceUser>,
}

fn get_stardance_users(
    stardance_api: &Url,
    stardance_api_key: &str,
    query: &str,
) -> Result<StardanceUsersResponse, anyhow::Error> {
    let client = reqwest::blocking::Client::new();
    let mut url = stardance_api.join("users")?;
    url.query_pairs_mut().append_pair("query", query);
    let response = client
        .get(url)
        .header("Authorization", format!("Bearer {}", stardance_api_key))
        .send()
        .context("Failed to fetch users from Stardance API")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Stardance API returned error: {} - {}",
            response.status(),
            response.text().unwrap_or_default()
        ));
    }
    let data: StardanceUsersResponse = response
        .json()
        .context("Invalid users response from Stardance API")?;

    Ok(data)
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS payout_session (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    completed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user (
    id INTEGER PRIMARY KEY,
    stardance_id INTEGER NOT NULL UNIQUE,
    slack_id TEXT NOT NULL UNIQUE
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
    let stardance_api = std::env::var("STARDANCE_API_URL")
        .context("STARDANCE_API_URL environment variable not set")?;
    let stardance_api =
        Url::parse(&stardance_api).context("STARDANCE_API_URL is not a valid URL")?;
    if stardance_api.path().trim_end_matches("/") != "/api/v1" {
        println!(
            "Warning: STARDANCE_API_URL does not end in `/api/v1`. Are you sure you have the full URL?"
        );
    }
    let stardance_api_key = std::env::var("STARDANCE_API_KEY")
        .context("STARDANCE_API_KEY environment variable not set")?;
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
    let mut nephthys_db = nephthys_db.context("failed to connect to the Nephthys database")?;
    let db = get_db(&args.database_file)?;

    let users =
        sync_helpers_from_nephthys(&mut nephthys_db, &db, &stardance_api, &stardance_api_key)?;

    let helper_tickets = get_helper_leaderboard(nephthys_db, None, None)?;

    println!("helper_tickets: {:?}", helper_tickets);
    println!("users: {:?}", users);

    Ok(())
}

/// Fetches helpers from Nephthys, looking up each helper's Stardance ID.
fn fetch_helpers_from_nephthys(
    nephthys_db: &mut Client,
    stardance_api: &Url,
    stardance_api_key: &str,
) -> Result<Vec<User>, anyhow::Error> {
    let rows = nephthys_db.query(
        r#"
        SELECT id, "slackId" AS slack_id
        FROM "User"
        WHERE helper = true;
    "#,
        &[],
    )?;

    let mut users = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get("id");
        let slack_id: &str = row.get("slack_id");

        let stardance_users = get_stardance_users(stardance_api, stardance_api_key, slack_id)?;
        let stardance_user = stardance_users
            .users
            .iter()
            .find(|u| u.slack_id == slack_id)
            .or_else(|| stardance_users.users.first())
            .ok_or_else(|| anyhow!("Stardance API returned no users for Slack ID {slack_id}"))?;

        users.push(User {
            id: id as u64,
            stardance_id: stardance_user.id as u64,
            slack_id: slack_id.to_string(),
        });
    }

    Ok(users)
}

/// Updates the local DB with helpers from Nephthys without deleting existing users.
fn sync_helpers_from_nephthys(
    nephthys_db: &mut Client,
    crimson_db: &Connection,
    stardance_api: &Url,
    stardance_api_key: &str,
) -> Result<Vec<User>, anyhow::Error> {
    let users = fetch_helpers_from_nephthys(nephthys_db, stardance_api, stardance_api_key)?;

    for user in &users {
        crimson_db.execute(
            r#"
            INSERT INTO user (id, stardance_id, slack_id)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(id) DO UPDATE SET
                stardance_id = excluded.stardance_id,
                slack_id = excluded.slack_id;
            "#,
            rusqlite::params![user.id as i64, user.stardance_id as i64, user.slack_id,],
        )?;
    }

    Ok(users)
}

/// Returns a map of Slack IDs to tickets closed
fn get_helper_leaderboard(
    mut db_client: Client,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
) -> Result<HashMap<String, i64>, anyhow::Error> {
    let start_time = start.unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let end_time = end.unwrap_or(OffsetDateTime::now_utc());
    let rows = db_client.query(
        r#"
        SELECT u."slackId" AS "slack_id", COUNT(*) AS "tickets_closed"
        FROM "Ticket" t
        JOIN "User" u ON u."id" = t."closedById"
        WHERE
            u."helper" = true
            AND t."closedAt" >= $1::timestamptz
            AND t."closedAt" < $2::timestamptz
        GROUP BY u."slackId"
        ORDER BY "tickets_closed" DESC;
    "#,
        &[&start_time, &end_time],
    )?;

    let hashmap: HashMap<String, i64> = rows
        .iter()
        .map(|row| {
            let slack_id: &str = row.get("slack_id");
            let tickets_closed: i64 = row.get("tickets_closed");
            (slack_id.to_string(), tickets_closed)
        })
        .collect();

    return Ok(hashmap);
}
