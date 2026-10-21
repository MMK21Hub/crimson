use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, Parser, Subcommand};
use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use reqwest::Url;
use rusqlite::Connection;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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
    /// Start time (ISO 8601, e.g. 2026-02-01T00:00:00Z). Required on the first regular run and for bonus payouts.
    #[arg(long)]
    start: Option<String>,

    /// End time (ISO 8601, e.g. 2026-03-01T00:00:00Z). Defaults to now for regular payouts; required for bonus payouts.
    #[arg(long)]
    end: Option<String>,

    /// Stardust paid out per closed ticket
    #[arg(long)]
    stardust_rate: f64,

    /// Make this a one-off bonus payout (requires --start, --end and --message)
    #[arg(long)]
    bonus: bool,

    /// Memo for a bonus payout (required with --bonus)
    #[arg(long)]
    message: Option<String>,

    /// Actually send payouts. Without this flag the command only reports what it would do.
    #[arg(long)]
    execute: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionType {
    Regular,
    Bonus,
}

impl SessionType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Bonus => "bonus",
        }
    }

    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "regular" => Ok(Self::Regular),
            "bonus" => Ok(Self::Bonus),
            other => Err(anyhow!("unknown payout session type `{other}`")),
        }
    }
}

#[derive(Debug, Clone)]
struct User {
    id: u64,
    stardance_id: u64,
    slack_id: String,
    excluded: bool,
}

struct Session {
    id: i64,
    kind: SessionType,
    period_start: OffsetDateTime,
    period_end: OffsetDateTime,
    stardust_rate: f64,
    started_at: OffsetDateTime,
    memo: Option<String>,
}

enum SessionRun {
    New {
        kind: SessionType,
        period_start: OffsetDateTime,
        period_end: OffsetDateTime,
        rate: f64,
        memo: Option<String>,
    },
    Resume(Session),
}

struct Payout {
    user: User,
    tickets: i64,
    amount: i64,
    unrounded_amount: f64,
    justification: String,
}

#[derive(Deserialize)]
struct StardanceUser {
    id: i64,
}

#[derive(serde::Serialize)]
struct BalanceAdjustment {
    amount: i64,
    reason: String,
}

#[derive(Deserialize)]
struct BalanceAdjustmentResponse {
    user_id: i64,
    amount: i64,
}

fn log(message: impl AsRef<str>) {
    eprintln!(
        "[{}] {}",
        format_time(OffsetDateTime::now_utc()),
        message.as_ref()
    );
}

fn format_time(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
}

const HUMAN_DATE_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[day padding:none] [month repr:short] [year]");

fn format_date(value: OffsetDateTime) -> String {
    value
        .to_offset(time::UtcOffset::UTC)
        .format(HUMAN_DATE_FORMAT)
        .unwrap_or_else(|_| format_time(value))
}

const NO_SECONDS_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]Z");

fn parse_time(value: &str) -> anyhow::Result<OffsetDateTime> {
    if let Ok(parsed) = OffsetDateTime::parse(value, &Rfc3339) {
        return Ok(parsed);
    }

    let naive = time::PrimitiveDateTime::parse(value, NO_SECONDS_FORMAT).with_context(|| {
        format!("invalid timestamp `{value}`; expected ISO 8601, e.g. 2026-02-01T00:00:00Z")
    })?;

    Ok(naive.assume_utc())
}

fn round_stardust(tickets: i64, rate: f64) -> anyhow::Result<i64> {
    let unrounded = tickets as f64 * rate;
    if !unrounded.is_finite() {
        return Err(anyhow!(
            "payout is not finite ({unrounded}) for {tickets} tickets at {rate} stardust/ticket"
        ));
    }

    let rounded = unrounded.round();
    if !(0.0..=i64::MAX as f64).contains(&rounded) {
        return Err(anyhow!(
            "payout {rounded} is out of range for {tickets} tickets at {rate} stardust/ticket"
        ));
    }

    Ok(rounded as i64)
}

fn justification(tickets: i64, rate: f64) -> String {
    format!("{tickets} tkts * {rate} sd/tkt")
}

fn payout_reason(
    period_start: OffsetDateTime,
    period_end: OffsetDateTime,
    tickets: i64,
    session_id: i64,
    memo: Option<&str>,
) -> String {
    match memo {
        Some(memo) => format!(
            "Support Scouts payout for {} start={} end={} tkts={tickets} session={session_id}",
            memo,
            period_start.to_string(),
            period_end
        ),
        None => format!(
            "Support Scouts payout from {} to {} tkts={tickets} session={session_id}",
            format_date(period_start),
            format_date(period_end)
        ),
    }
}

fn get_stardance_user(
    stardance_api: &Url,
    stardance_api_key: &str,
    slack_id: &str,
) -> Result<StardanceUser, anyhow::Error> {
    let client = reqwest::blocking::Client::new();
    let base = format!("{}/", stardance_api.as_str().trim_end_matches('/'));
    let mut url = Url::parse(&base)?.join("users/lookup")?;
    url.query_pairs_mut().append_pair("slack_id", slack_id);
    let response = client
        .get(url)
        .header("Authorization", format!("Bearer {}", stardance_api_key))
        .send()
        .context("Failed to fetch user from Stardance API")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(anyhow!("No Stardance user found for Slack ID {slack_id}"));
    }
    if !response.status().is_success() {
        return Err(anyhow!(
            "Stardance API returned error: {} - {}",
            response.status(),
            response.text().unwrap_or_default()
        ));
    }
    let data: StardanceUser = response
        .json()
        .context("Invalid user response from Stardance API")?;

    Ok(data)
}

fn post_balance_adjustment(
    stardance_api: &Url,
    stardance_api_key: &str,
    stardance_id: u64,
    amount: i64,
    reason: &str,
) -> anyhow::Result<()> {
    let client = reqwest::blocking::Client::new();
    let base = format!("{}/", stardance_api.as_str().trim_end_matches('/'));
    let url = Url::parse(&base)?.join(&format!("users/{stardance_id}/balance_adjustments"))?;

    let response = client
        .post(url)
        .header("Authorization", format!("Bearer {}", stardance_api_key))
        .json(&BalanceAdjustment {
            amount,
            reason: reason.to_string(),
        })
        .send()
        .context("Failed to create Stardance balance adjustment")?;

    if !response.status().is_success() {
        return Err(anyhow!(
            "Stardance API returned error: {} - {}",
            response.status(),
            response.text().unwrap_or_default()
        ));
    }

    let data: BalanceAdjustmentResponse = response
        .json()
        .context("Invalid balance adjustment response from Stardance API")?;

    let expected_user_id = i64::try_from(stardance_id)
        .context("Stardance ID does not fit in a signed 64-bit integer")?;
    if data.user_id != expected_user_id || data.amount != amount {
        return Err(anyhow!(
            "unexpected balance adjustment response: expected user {expected_user_id} amount {amount}, got user {} amount {}",
            data.user_id,
            data.amount
        ));
    }

    Ok(())
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS payout_session (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL DEFAULT 'regular' CHECK (type IN ('regular', 'bonus')),
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    stardust_rate REAL NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS user (
    id INTEGER PRIMARY KEY,
    stardance_id INTEGER NOT NULL UNIQUE,
    slack_id TEXT NOT NULL UNIQUE,
    excluded INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS payout_transaction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session INTEGER NOT NULL REFERENCES payout_session(id),
    user INTEGER NOT NULL REFERENCES user(id),
    amount INTEGER NOT NULL,
    unrounded_amount REAL NOT NULL,
    justification TEXT NOT NULL,
    UNIQUE(session, user)
);
"#;

fn get_db(path: &Path) -> anyhow::Result<Connection> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create database directory {}", parent.display()))?;
    }

    let conn = Connection::open(path)
        .with_context(|| format!("Failed to open database {}", path.display()))?;

    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", true)?;
    conn.execute_batch(SCHEMA)?;

    Ok(conn)
}

fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let nephthys_db_url =
        std::env::var("NEPHTHYS_DB_URL").context("NEPHTHYS_DB_URL environment variable not set")?;
    let stardance_api = std::env::var("STARDANCE_API_URL")
        .unwrap_or_else(|_| "https://stardance.hackclub.com/api/v1".to_string());
    let stardance_api =
        Url::parse(&stardance_api).context("STARDANCE_API_URL is not a valid URL")?;
    if stardance_api.path().trim_end_matches("/") != "/api/v1" {
        log(
            "warning: STARDANCE_API_URL does not end in `/api/v1`. Are you sure you have the full URL?",
        );
    }
    let stardance_api_key = std::env::var("STARDANCE_API_KEY")
        .context("STARDANCE_API_KEY environment variable not set")?;
    let args = CrimsonArgs::parse();

    let nephthys_db = if let Ok(connector) = TlsConnector::new() {
        let tls = MakeTlsConnector::new(connector);
        postgres::Client::connect(&nephthys_db_url, tls)
    } else {
        log(
            "warning: failed to initialise TLS (no native TLS lib available?), falling back to unencrypted connection",
        );
        postgres::Client::connect(&nephthys_db_url, postgres::NoTls)
    };
    let mut nephthys_db = nephthys_db.context("failed to connect to the Nephthys database")?;

    match args.command {
        Command::Payout(payout_args) => run_payout(
            &args.database_file,
            payout_args,
            &mut nephthys_db,
            &stardance_api,
            &stardance_api_key,
        )?,
    }

    Ok(())
}

fn run_payout(
    database_file: &Path,
    args: PayoutArgs,
    nephthys_db: &mut Client,
    stardance_api: &Url,
    stardance_api_key: &str,
) -> anyhow::Result<()> {
    let db = get_db(database_file)?;

    let (kind, period_start, period_end, rate, memo, resume) = match resolve_session(&db, &args)? {
        SessionRun::New {
            kind,
            period_start,
            period_end,
            rate,
            memo,
        } => {
            match &memo {
                Some(message) => log(format!(
                    "starting bonus payout period {} .. {} at {rate} stardust/ticket ({message})",
                    format_time(period_start),
                    format_time(period_end)
                )),
                None => log(format!(
                    "starting payout period {} .. {} at {rate} stardust/ticket",
                    format_time(period_start),
                    format_time(period_end)
                )),
            }
            (kind, period_start, period_end, rate, memo, None)
        }
        SessionRun::Resume(session) => {
            let memo_description = session
                .memo
                .as_deref()
                .map(|message| format!(" ({message})"))
                .unwrap_or_default();
            log(format!(
                "resuming {} payout session {} (started {}), period {} .. {} at {} stardust/ticket{}",
                session.kind.as_str(),
                session.id,
                format_time(session.started_at),
                format_time(session.period_start),
                format_time(session.period_end),
                session.stardust_rate,
                memo_description
            ));
            let memo = session.memo.clone();
            let kind = session.kind;
            (
                kind,
                session.period_start,
                session.period_end,
                session.stardust_rate,
                memo,
                Some(session),
            )
        }
    };

    let ticket_counts = get_helper_leaderboard(nephthys_db, period_start, period_end)?;
    let total_tickets: i64 = ticket_counts.values().sum();
    log(format!(
        "{} helpers closed {} tickets during this period",
        ticket_counts.len(),
        total_tickets
    ));

    if !args.execute {
        dry_run(&db, &ticket_counts, rate)?;
        log("dry run complete: no Stardance API calls or database writes were performed");
        return Ok(());
    }

    let users_by_slack = sync_helpers_from_nephthys(
        nephthys_db,
        &db,
        stardance_api,
        stardance_api_key,
        &ticket_counts,
    )?;

    let payouts = build_payouts(&ticket_counts, &users_by_slack, rate)?;

    let (session_id, paid_users) = match resume {
        Some(session) => {
            let paid = load_paid_user_ids(&db, session.id)?;
            log(format!(
                "session {} already has {} recorded payouts",
                session.id,
                paid.len()
            ));
            (session.id, paid)
        }
        None => {
            db.execute(
                "INSERT INTO payout_session (type, period_start, period_end, stardust_rate, started_at, memo) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    kind.as_str(),
                    format_time(period_start),
                    format_time(period_end),
                    rate,
                    format_time(OffsetDateTime::now_utc()),
                    memo.as_deref()
                ],
            )
            .context("failed to create payout session")?;
            let id = db.last_insert_rowid();
            log(format!("created {} payout session {id}", kind.as_str()));
            (id, HashSet::new())
        }
    };

    let mut paid_count = 0usize;
    let mut paid_stardust = 0i64;
    for payout in &payouts {
        let user_id = i64::try_from(payout.user.id)
            .context("local user ID does not fit in a signed 64-bit integer")?;

        if paid_users.contains(&user_id) {
            log(format!(
                "{} (Stardance #{}) already paid in session {session_id}, skipping",
                payout.user.slack_id, payout.user.stardance_id
            ));
            continue;
        }

        let reason = payout_reason(
            period_start,
            period_end,
            payout.tickets,
            session_id,
            memo.as_deref(),
        );
        log(&reason);

        post_balance_adjustment(
            stardance_api,
            stardance_api_key,
            payout.user.stardance_id,
            payout.amount,
            &reason,
        )
        .with_context(|| {
            format!(
                "failed to pay {} (Stardance #{}): {reason}",
                payout.user.slack_id, payout.user.stardance_id
            )
        })?;

        db.execute(
            "INSERT INTO payout_transaction (session, user, amount, unrounded_amount, justification) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                session_id,
                user_id,
                payout.amount,
                payout.unrounded_amount,
                payout.justification
            ],
        )
        .with_context(|| format!("failed to record payout for {}", payout.user.slack_id))?;

        log(format!(
            "paid {} (Stardance #{}) {} stardust: {reason}",
            payout.user.slack_id, payout.user.stardance_id, payout.amount
        ));
        paid_count += 1;
        paid_stardust += payout.amount;
    }

    db.execute(
        "UPDATE payout_session SET completed_at = ?1 WHERE id = ?2",
        rusqlite::params![format_time(OffsetDateTime::now_utc()), session_id],
    )
    .context("failed to mark payout session as complete")?;

    log(format!(
        "completed payout session {session_id}: {paid_count} payouts, {paid_stardust} stardust"
    ));

    Ok(())
}

fn dry_run(db: &Connection, ticket_counts: &HashMap<String, i64>, rate: f64) -> anyhow::Result<()> {
    let users = load_local_users(db)?;
    let mut sorted: Vec<(&String, &i64)> = ticket_counts.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    let mut resolved = 0usize;
    let mut unknown_users = 0usize;
    let mut excluded = 0usize;
    let mut total_stardust = 0i64;

    for (slack_id, tickets) in sorted {
        match users.get(slack_id) {
            Some(user) => {
                if user.excluded {
                    excluded += 1;
                    log(format!(
                        "{slack_id}: excluded from payouts ({tickets} tickets ignored)"
                    ));
                    continue;
                }
                resolved += 1;
                let amount = round_stardust(*tickets, rate)?;
                let unrounded = format!("{:.2}", *tickets as f64 * rate);
                if amount == 0 {
                    log(format!(
                        "{slack_id}: {tickets} tickets * {rate} = {unrounded} -> 0 stardust (skipped)"
                    ));
                } else {
                    log(format!(
                        "{slack_id} (Stardance #{}): {tickets} tickets * {rate} = {unrounded} -> {amount} stardust",
                        user.stardance_id
                    ));
                    total_stardust += amount;
                }
            }
            None => {
                unknown_users += 1;
                log(format!(
                    "{slack_id}: {tickets} tickets - Stardance ID unknown; would be resolved during --execute"
                ));
            }
        }
    }

    log(format!(
        "dry run summary: {resolved} helpers with known Stardance IDs, {unknown_users} unknown users, {excluded} excluded, {total_stardust} stardust would be paid"
    ));

    Ok(())
}

fn build_payouts(
    ticket_counts: &HashMap<String, i64>,
    users: &HashMap<String, User>,
    rate: f64,
) -> anyhow::Result<Vec<Payout>> {
    let mut sorted: Vec<(&String, &i64)> = ticket_counts.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    let mut payouts = Vec::with_capacity(sorted.len());
    for (slack_id, tickets) in sorted {
        let user = users
            .get(slack_id)
            .ok_or_else(|| anyhow!("no local user record found for helper {slack_id}"))?
            .clone();
        if user.excluded {
            log(format!("skipping {slack_id}: excluded from payouts"));
            continue;
        }
        let amount = round_stardust(*tickets, rate)?;
        if amount == 0 {
            log(format!(
                "skipping {slack_id}: {tickets} tickets at {rate} sd/tkt rounds to 0 stardust"
            ));
            continue;
        }
        payouts.push(Payout {
            user,
            tickets: *tickets,
            amount,
            unrounded_amount: *tickets as f64 * rate,
            justification: justification(*tickets, rate),
        });
    }

    Ok(payouts)
}

fn load_local_users(db: &Connection) -> anyhow::Result<HashMap<String, User>> {
    let mut stmt = db.prepare("SELECT id, stardance_id, slack_id, excluded FROM user")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, bool>(3)?,
        ))
    })?;

    let mut users = HashMap::new();
    for row in rows {
        let (id, stardance_id, slack_id, excluded) = row?;
        let user = User {
            id: u64::try_from(id).context("local user ID is negative")?,
            stardance_id: u64::try_from(stardance_id).context("local Stardance ID is negative")?,
            slack_id: slack_id.clone(),
            excluded,
        };
        users.insert(slack_id, user);
    }

    Ok(users)
}

fn load_excluded_slack_ids(db: &Connection) -> anyhow::Result<HashSet<String>> {
    let mut stmt = db.prepare("SELECT slack_id FROM user WHERE excluded = 1")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

    let mut slack_ids = HashSet::new();
    for row in rows {
        slack_ids.insert(row?);
    }

    Ok(slack_ids)
}

fn load_paid_user_ids(db: &Connection, session_id: i64) -> anyhow::Result<HashSet<i64>> {
    let mut stmt = db.prepare("SELECT user FROM payout_transaction WHERE session = ?1")?;
    let rows = stmt.query_map([session_id], |row| row.get::<_, i64>(0))?;

    let mut user_ids = HashSet::new();
    for row in rows {
        user_ids.insert(row?);
    }

    Ok(user_ids)
}

type SessionRow = (i64, String, String, String, f64, String, Option<String>);

fn parse_session(row: SessionRow) -> anyhow::Result<Session> {
    let (id, kind, period_start, period_end, stardust_rate, started_at, memo) = row;

    Ok(Session {
        id,
        kind: SessionType::parse(&kind)
            .with_context(|| format!("payout session {id} has an invalid type"))?,
        period_start: parse_time(&period_start)
            .with_context(|| format!("payout session {id} has an invalid period_start"))?,
        period_end: parse_time(&period_end)
            .with_context(|| format!("payout session {id} has an invalid period_end"))?,
        stardust_rate,
        started_at: parse_time(&started_at)
            .with_context(|| format!("payout session {id} has an invalid started_at"))?,
        memo,
    })
}

fn query_sessions(db: &Connection, sql: &str) -> anyhow::Result<Vec<Session>> {
    let mut stmt = db.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, f64>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
        ))
    })?;

    let mut sessions = Vec::new();
    for row in rows {
        sessions.push(parse_session(row?)?);
    }

    Ok(sessions)
}

fn resolve_session(db: &Connection, args: &PayoutArgs) -> anyhow::Result<SessionRun> {
    if !args.stardust_rate.is_finite() || args.stardust_rate <= 0.0 {
        return Err(anyhow!(
            "--stardust-rate must be a positive number, got {}",
            args.stardust_rate
        ));
    }

    let requested = if args.bonus {
        SessionType::Bonus
    } else {
        SessionType::Regular
    };

    let completed_regular = if requested == SessionType::Regular {
        query_sessions(
            db,
            "SELECT id, type, period_start, period_end, stardust_rate, started_at, memo
             FROM payout_session
             WHERE completed_at IS NOT NULL AND type = 'regular'
             ORDER BY id",
        )?
    } else {
        Vec::new()
    };

    for pair in completed_regular.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        if next.period_start < previous.period_end {
            return Err(anyhow!(
                "payout sessions are out of order: session {} covers {} .. {} but session {} starts earlier at {}; resolve the session history manually before running again",
                previous.id,
                format_time(previous.period_start),
                format_time(previous.period_end),
                next.id,
                format_time(next.period_start)
            ));
        }
    }

    let incomplete = query_sessions(
        db,
        "SELECT id, type, period_start, period_end, stardust_rate, started_at, memo
         FROM payout_session
         WHERE completed_at IS NULL
         ORDER BY id",
    )?;

    if incomplete.len() > 1 {
        let sessions: Vec<String> = incomplete
            .iter()
            .map(|session| format!("{} ({})", session.id, session.kind.as_str()))
            .collect();
        return Err(anyhow!(
            "found multiple incomplete payout sessions ({}); resolve them manually before running again",
            sessions.join(", ")
        ));
    }

    if let Some(session) = incomplete.into_iter().next() {
        if session.kind != requested {
            let resume_hint = match session.kind {
                SessionType::Bonus => "rerun with --bonus to resume it",
                SessionType::Regular => "rerun without --bonus to resume it",
            };
            return Err(anyhow!(
                "payout session {} is an incomplete {} session; {resume_hint}",
                session.id,
                session.kind.as_str()
            ));
        }
        if args.start.is_some() || args.end.is_some() {
            return Err(anyhow!(
                "payout session {} is incomplete; rerun without --start/--end to resume it",
                session.id
            ));
        }
        if (session.stardust_rate - args.stardust_rate).abs() > 1e-9 {
            return Err(anyhow!(
                "payout session {} was started at {} stardust/ticket, but --stardust-rate {} was given; rerun with the original rate",
                session.id,
                session.stardust_rate,
                args.stardust_rate
            ));
        }
        match (requested, args.message.as_deref()) {
            (SessionType::Regular, Some(_)) => {
                return Err(anyhow!("--message is only valid with --bonus"));
            }
            (SessionType::Bonus, Some(message))
                if Some(message.trim()) != session.memo.as_deref() =>
            {
                return Err(anyhow!(
                    "payout session {} used the memo `{}`, but --message `{}` was given; rerun with the original memo or omit it",
                    session.id,
                    session.memo.as_deref().unwrap_or_default(),
                    message.trim()
                ));
            }
            _ => {}
        }
        return Ok(SessionRun::Resume(session));
    }

    if requested == SessionType::Bonus {
        let start = args
            .start
            .as_deref()
            .ok_or_else(|| anyhow!("bonus payouts require both --start and --end"))?;
        let end = args
            .end
            .as_deref()
            .ok_or_else(|| anyhow!("bonus payouts require both --start and --end"))?;
        let memo = args
            .message
            .as_deref()
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .ok_or_else(|| anyhow!("bonus payouts require a non-empty --message"))?;

        let period_start = parse_time(start)?;
        let period_end = parse_time(end)?;
        if period_end <= period_start {
            return Err(anyhow!(
                "period end {} must be after period start {}",
                format_time(period_end),
                format_time(period_start)
            ));
        }

        return Ok(SessionRun::New {
            kind: SessionType::Bonus,
            period_start,
            period_end,
            rate: args.stardust_rate,
            memo: Some(memo.to_string()),
        });
    }

    if args.message.is_some() {
        return Err(anyhow!("--message is only valid with --bonus"));
    }

    let last_completed = completed_regular.last();

    let period_start = match (&args.start, last_completed) {
        (Some(value), _) => parse_time(value)?,
        (None, Some(session)) => session.period_end,
        (None, None) => {
            return Err(anyhow!(
                "no previous payout session found; pass --start to begin the first payout period"
            ));
        }
    };

    if let Some(session) = last_completed
        && period_start < session.period_end
    {
        return Err(anyhow!(
            "period start {} overlaps the last completed session {} (which ended at {}); pass a later --start or omit it to continue from there",
            format_time(period_start),
            session.id,
            format_time(session.period_end)
        ));
    }

    let period_end = match &args.end {
        Some(value) => parse_time(value)?,
        None => OffsetDateTime::now_utc(),
    };

    if period_end <= period_start {
        return Err(anyhow!(
            "period end {} must be after period start {}",
            format_time(period_end),
            format_time(period_start)
        ));
    }

    Ok(SessionRun::New {
        kind: SessionType::Regular,
        period_start,
        period_end,
        rate: args.stardust_rate,
        memo: None,
    })
}

/// Fetches helpers from Nephthys, looking up each helper's Stardance ID.
fn fetch_helpers_from_nephthys(
    nephthys_db: &mut Client,
    stardance_api: &Url,
    stardance_api_key: &str,
    helper_tickets: &HashMap<String, i64>,
    excluded_slack_ids: &HashSet<String>,
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
        let id: i32 = row.try_get("id")?;
        let slack_id: &str = row.try_get("slack_id")?;

        if helper_tickets.get(slack_id).copied().unwrap_or(0) == 0 {
            continue;
        }

        if excluded_slack_ids.contains(slack_id) {
            continue;
        }

        let stardance_user = get_stardance_user(stardance_api, stardance_api_key, slack_id)
            .with_context(|| format!("Failed to resolve Stardance user for Nephthys user {id}"))?;

        users.push(User {
            id: u64::try_from(id).context("Nephthys returned a negative user ID")?,
            stardance_id: u64::try_from(stardance_user.id)
                .context("Stardance API returned a negative user ID")?,
            slack_id: slack_id.to_string(),
            excluded: false,
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
    helper_tickets: &HashMap<String, i64>,
) -> Result<HashMap<String, User>, anyhow::Error> {
    let excluded_slack_ids = load_excluded_slack_ids(crimson_db)?;
    let users = fetch_helpers_from_nephthys(
        nephthys_db,
        stardance_api,
        stardance_api_key,
        helper_tickets,
        &excluded_slack_ids,
    )?;

    for user in &users {
        let id = i64::try_from(user.id).context("local user ID does not fit in INTEGER")?;
        let stardance_id =
            i64::try_from(user.stardance_id).context("Stardance ID does not fit in INTEGER")?;

        crimson_db.execute(
            r#"
            INSERT INTO user (id, stardance_id, slack_id)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(id) DO UPDATE SET
                stardance_id = excluded.stardance_id,
                slack_id = excluded.slack_id;
            "#,
            rusqlite::params![id, stardance_id, user.slack_id],
        )?;
    }

    load_local_users(crimson_db)
}

/// Returns a map of Slack IDs to tickets closed during the period
fn get_helper_leaderboard(
    db_client: &mut Client,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<HashMap<String, i64>, anyhow::Error> {
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
        &[&start, &end],
    )?;

    let mut hashmap: HashMap<String, i64> = HashMap::with_capacity(rows.len());
    for row in &rows {
        let slack_id: &str = row.try_get("slack_id")?;
        let tickets_closed: i64 = row.try_get("tickets_closed")?;
        hashmap.insert(slack_id.to_string(), tickets_closed);
    }

    Ok(hashmap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamps_with_and_without_seconds() {
        let with_seconds = parse_time("2026-09-16T00:00:00Z").expect("valid timestamp");
        let without_seconds = parse_time("2026-09-16T00:00Z").expect("valid timestamp");
        assert_eq!(with_seconds, without_seconds);
    }

    #[test]
    fn rejects_invalid_timestamps() {
        assert!(parse_time("not a time").is_err());
    }

    #[test]
    fn rounds_stardust_to_nearest() {
        assert_eq!(round_stardust(3, 0.3).expect("valid"), 1);
        assert_eq!(round_stardust(7, 0.2).expect("valid"), 1);
        assert_eq!(round_stardust(5, 0.3).expect("valid"), 2);
        assert_eq!(round_stardust(1, 0.4).expect("valid"), 0);
    }

    #[test]
    fn formats_justification() {
        assert_eq!(justification(45, 0.3), "45 tkts * 0.3 sd/tkt");
    }

    #[test]
    fn formats_payout_reason() {
        let start = parse_time("2026-09-16T00:00:00Z").expect("valid timestamp");
        let end = parse_time("2026-10-10T00:00:00Z").expect("valid timestamp");

        assert_eq!(
            payout_reason(start, end, 52, 1, None),
            "Support Scouts payout from 16 Sep 2026 to 10 Oct 2026 tkts=52 session=1"
        );
        assert_eq!(
            payout_reason(start, end, 52, 1, Some("Tuesday lock-in huddle")),
            "Support Scouts payout from 16 Sep 2026 to 10 Oct 2026 tkts=52 session=1 \"Tuesday lock-in huddle\""
        );
    }

    #[test]
    fn skips_excluded_users() {
        let user = User {
            id: 1,
            stardance_id: 158,
            slack_id: "U073M5L9U13".to_string(),
            excluded: true,
        };
        let users = HashMap::from([("U073M5L9U13".to_string(), user)]);
        let tickets = HashMap::from([("U073M5L9U13".to_string(), 25)]);

        let payouts = build_payouts(&tickets, &users, 0.4).expect("build payouts");
        assert!(payouts.is_empty());
    }

    #[test]
    fn loads_excluded_slack_ids() {
        let db = test_db();
        db.execute(
            "INSERT INTO user (id, stardance_id, slack_id, excluded) VALUES (1, 158, 'U1', 1), (2, 159, 'U2', 0)",
            [],
        )
        .expect("insert users");

        assert_eq!(
            load_excluded_slack_ids(&db).expect("load excluded users"),
            HashSet::from(["U1".to_string()])
        );
    }

    fn test_db() -> Connection {
        let db = Connection::open_in_memory().expect("open in-memory database");
        db.execute_batch(SCHEMA).expect("create schema");
        db
    }

    fn insert_session(db: &Connection, completed: bool) -> i64 {
        insert_session_with_period(
            db,
            "2026-10-01T00:00:00Z",
            "2026-10-08T00:00:00Z",
            completed,
        )
    }

    fn insert_session_with_period(
        db: &Connection,
        period_start: &str,
        period_end: &str,
        completed: bool,
    ) -> i64 {
        insert_session_of_type(
            db,
            SessionType::Regular,
            period_start,
            period_end,
            completed,
            None,
        )
    }

    fn insert_session_of_type(
        db: &Connection,
        kind: SessionType,
        period_start: &str,
        period_end: &str,
        completed: bool,
        memo: Option<&str>,
    ) -> i64 {
        db.execute(
            "INSERT INTO payout_session (type, period_start, period_end, stardust_rate, started_at, completed_at, memo) VALUES (?1, ?2, ?3, 0.4, ?4, ?5, ?6)",
            rusqlite::params![
                kind.as_str(),
                period_start,
                period_end,
                period_end,
                completed.then_some(period_end),
                memo
            ],
        )
        .expect("insert session");
        db.last_insert_rowid()
    }

    #[test]
    fn resumes_incomplete_session() {
        let db = test_db();
        let id = insert_session(&db, false);
        let args = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: false,
            message: None,
            execute: false,
        };

        let resolved = resolve_session(&db, &args).expect("resolve session");
        assert!(
            matches!(resolved, SessionRun::Resume(ref session) if session.id == id),
            "expected to resume session {id}"
        );
    }

    #[test]
    fn continues_from_last_completed_session() {
        let db = test_db();
        insert_session(&db, true);
        let expected_start = parse_time("2026-10-08T00:00:00Z").expect("valid timestamp");
        let args = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: false,
            message: None,
            execute: false,
        };

        let resolved = resolve_session(&db, &args).expect("resolve session");
        assert!(
            matches!(
                resolved,
                SessionRun::New { period_start, .. } if period_start == expected_start
            ),
            "expected a new session starting at the last session's end"
        );
    }

    #[test]
    fn rejects_out_of_order_completed_sessions() {
        let db = test_db();
        insert_session_with_period(&db, "2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z", true);
        insert_session_with_period(&db, "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z", true);

        let args = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: false,
            message: None,
            execute: false,
        };

        assert!(resolve_session(&db, &args).is_err());
    }

    #[test]
    fn loads_paid_user_ids_for_session() {
        let db = test_db();
        let session_id = insert_session(&db, false);
        db.execute(
            "INSERT INTO user (id, stardance_id, slack_id) VALUES (158, 158, 'U073M5L9U13')",
            [],
        )
        .expect("insert user");
        db.execute(
            "INSERT INTO payout_transaction (session, user, amount, unrounded_amount, justification) VALUES (?1, 158, 10, 10.0, '25 tkts * 0.4 sd/tkt')",
            [session_id],
        )
        .expect("insert transaction");

        let paid = load_paid_user_ids(&db, session_id).expect("load paid users");
        assert_eq!(paid, HashSet::from([158]));
    }

    #[test]
    fn bonus_requires_start_end_and_message() {
        let db = test_db();
        let args = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: true,
            message: None,
            execute: false,
        };

        assert!(resolve_session(&db, &args).is_err());
    }

    #[test]
    fn regular_ignores_completed_bonus_sessions() {
        let db = test_db();
        insert_session_with_period(&db, "2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z", true);
        insert_session_of_type(
            &db,
            SessionType::Bonus,
            "2026-11-01T00:00:00Z",
            "2026-11-08T00:00:00Z",
            true,
            Some("Tuesday lock-in huddle"),
        );

        let expected_start = parse_time("2026-10-08T00:00:00Z").expect("valid timestamp");
        let args = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: false,
            message: None,
            execute: false,
        };

        let resolved = resolve_session(&db, &args).expect("resolve session");
        assert!(
            matches!(
                resolved,
                SessionRun::New { period_start, .. } if period_start == expected_start
            ),
            "regular payouts should ignore bonus sessions"
        );
    }

    #[test]
    fn incomplete_bonus_blocks_regular_and_is_resumed_by_bonus() {
        let db = test_db();
        let id = insert_session_of_type(
            &db,
            SessionType::Bonus,
            "2026-10-01T00:00:00Z",
            "2026-10-08T00:00:00Z",
            false,
            Some("Tuesday lock-in huddle"),
        );

        let regular = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: false,
            message: None,
            execute: false,
        };
        assert!(resolve_session(&db, &regular).is_err());

        let bonus = PayoutArgs {
            start: None,
            end: None,
            stardust_rate: 0.4,
            bonus: true,
            message: None,
            execute: false,
        };
        let resolved = resolve_session(&db, &bonus).expect("resolve session");
        assert!(
            matches!(resolved, SessionRun::Resume(ref session) if session.id == id),
            "expected to resume the bonus session {id}"
        );
    }
}
