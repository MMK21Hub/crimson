use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use postgres::{Client, NoTls};
use reqwest::Url;
use serde::Deserialize;
use time::macros::{datetime, format_description};
use time::{OffsetDateTime, PrimitiveDateTime};

#[derive(Parser)]
struct CrimsonArgs {
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
    start: String,

    /// End time (ISO 6801, e.g. 2026-03-01T00:00:00Z)
    #[arg(long)]
    end: String,
}

fn parse_datetime(s: &str) -> Result<OffsetDateTime> {
    let datetime =
        OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
            .context("Invalid datetime string")?;
    Ok(datetime)
}

fn main() -> anyhow::Result<()> {
    // Configuration
    dotenvy::dotenv().ok();
    let db_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;
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
    let command_args: &PayoutArgs = match &args.command {
        Command::Payout(p) => p,
    };
    let start = parse_datetime(&command_args.start)?;
    let end = parse_datetime(&command_args.end)?;
    let pretty_printer = format_description!(
        "[weekday] [day padding:none] [month repr:short] [year] (@ [hour]:[minute])"
    );
    println!(
        "Selecting leaderboard from {} to {} (Period: {})",
        start.format(&pretty_printer)?,
        end.format(&pretty_printer)?,
        end - start
    );

    // let client =
    //     Client::connect(&db_url, NoTls).context("Failed to connect to Nephthys database")?;

    // let x = get_helper_leaderboard(client, start, end)?;
    let users = get_flavortown_users(&flavortown_api, &flavortown_api_key, "U073M5L9U13")?.users;
    let user = users.get(0).context("Flavortown API returned no users")?;
    println!("Flavortown user: {:?}", user);

    Ok(())
}

fn get_helper_leaderboard(
    mut client: Client,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<std::collections::HashMap<String, i64>, anyhow::Error> {
    let start_time = start;
    let end_time = end;
    let rows = client.query(
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

    let hashmap = rows
        .iter()
        .map(|row| {
            let slack_id: &str = row.get("slack_id");
            let tickets_closed: i64 = row.get("tickets_closed");
            (slack_id.to_string(), tickets_closed)
        })
        .collect::<std::collections::HashMap<String, i64>>();

    return Ok(hashmap);
}

#[derive(Deserialize, Debug)]
struct FlavortownUser {
    id: i64,
    slack_id: String,
    display_name: String,
    avatar: String,
    project_ids: Vec<i64>,
    cookies: Option<i64>,
}
#[derive(Deserialize, Debug)]
struct FlavortownUsersResponse {
    users: Vec<FlavortownUser>,
}

fn get_flavortown_users(
    flavortown_api: &Url,
    flavortown_api_key: &str,
    query: &str,
) -> Result<FlavortownUsersResponse, anyhow::Error> {
    let client = reqwest::blocking::Client::new();
    let mut url = flavortown_api.join("users")?;
    url.query_pairs_mut().append_pair("query", query);
    println!("Fetching users from Flavortown API: {}", url);
    let response = client
        .get(url)
        .header("Authorization", format!("Bearer {}", flavortown_api_key))
        .send()
        .context("Failed to fetch users from Flavortown API")?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Flavortown API returned error: {} - {}",
            response.status(),
            response.text().unwrap_or_default()
        ));
    }
    let data: FlavortownUsersResponse = response
        .json()
        .context("Invalid users response from Flavortown API")?;

    Ok(data)
}
