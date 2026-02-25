use std::collections::HashMap;

use anyhow::{Context, Ok, Result};
use clap::{Args, Parser, Subcommand};
use postgres::{Client, NoTls};
use reqwest::Url;
use serde::Deserialize;
use time::OffsetDateTime;
use time::macros::format_description;

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

    #[clap(flatten)]
    payout_specifier: PayoutSpecifierArgs,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
pub struct PayoutSpecifierArgs {
    /// Pays out helpers at a fixed rate of X cookies per ticket
    #[clap(long)]
    cookie_rate: Option<f64>,
    /// Pays out helpers based on a cookie pool of X cookies, distributed proportionally to the number of tickets closed
    #[clap(long)]
    cookie_pool: Option<i32>,
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

    let client =
        Client::connect(&db_url, NoTls).context("Failed to connect to Nephthys database")?;

    let helper_tickets = get_helper_leaderboard(client, start, end)?;

    if let Some(payout_rate) = command_args.payout_specifier.cookie_rate {
        do_static_rate_payouts(
            helper_tickets,
            payout_rate,
            flavortown_api,
            flavortown_api_key,
        )
    } else if let Some(pool) = command_args.payout_specifier.cookie_pool {
        do_pool_payouts(helper_tickets, pool, flavortown_api, flavortown_api_key)
    } else {
        unreachable!("One of cookie_rate or cookie_pool should be set")
    }
}

fn do_pool_payouts(
    helper_tickets: HashMap<String, i64>,
    pool: i32,
    flavortown_api: Url,
    flavortown_api_key: String,
) -> Result<(), anyhow::Error> {
    let total_tickets_closed: i64 = helper_tickets.values().sum();
    let helper_cookies: HashMap<&String, f64> = helper_tickets
        .iter()
        .map(|(id, tickets)| {
            let payout = (*tickets as f64 / total_tickets_closed as f64) * pool as f64;
            (id, payout)
        })
        .collect();
    print_helper_cookies(
        helper_cookies,
        &helper_tickets,
        flavortown_api,
        flavortown_api_key,
    )?;
    Ok(())
}

fn do_static_rate_payouts(
    helper_tickets: HashMap<String, i64>,
    payout_rate: f64,
    flavortown_api: Url,
    flavortown_api_key: String,
) -> Result<(), anyhow::Error> {
    let helper_cookies: HashMap<&String, f64> = helper_tickets
        .iter()
        .map(|(id, tickets)| (id, (*tickets as f64) * payout_rate))
        .collect();
    print_helper_cookies(
        helper_cookies,
        &helper_tickets,
        flavortown_api,
        flavortown_api_key,
    )?;
    Ok(())
}

fn print_helper_cookies(
    helper_cookies: HashMap<&String, f64>,
    helper_tickets: &HashMap<String, i64>,
    flavortown_api: Url,
    flavortown_api_key: String,
) -> Result<(), anyhow::Error> {
    println!(
        "Total tickets closed: {}",
        helper_tickets.values().sum::<i64>()
    );
    println!(
        "Total cookies to pay out: {}",
        helper_cookies.values().sum::<f64>()
    );
    println!();

    let mut helper_cookies_vec: Vec<(&&String, &f64)> = helper_cookies.iter().collect();
    helper_cookies_vec.sort_by(|(_, cookies_a), (_, cookies_b)| {
        cookies_b
            .partial_cmp(cookies_a)
            .expect("unexpected unorderable float")
    });
    for (slack_id, cookies) in helper_cookies_vec {
        let matching_users =
            get_flavortown_users(&flavortown_api, &flavortown_api_key, slack_id)?.users;
        let user = matching_users
            .get(0)
            .context("Flavortown API returned no users")?;
        println!(
            "{}: {} gets {} cookies! ({} tkts)\n",
            user.display_name,
            format!("https://flavortown.hackclub.com/admin/users/{}", user.id),
            (*cookies as f32), // use f32 to reduce the chances of .0000000000001
            helper_tickets.get(*slack_id).unwrap_or(&0)
        );
    }
    Ok(())
}

fn get_helper_leaderboard(
    mut client: Client,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<HashMap<String, i64>, anyhow::Error> {
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

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
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
