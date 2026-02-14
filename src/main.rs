use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use postgres::{Client, NoTls};
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

    let client = Client::connect(&db_url, NoTls)?;

    let x = get_helper_leaderboard(client, start, end)?;

    println!("{:#?}", x);

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
