use std::collections::HashMap;

use anyhow::{Context, Ok, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use postgres::{Client, NoTls};
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

    /// Optional user(s) to give a "bonus payout" to
    #[arg(long, num_args = 0..)]
    bonus_users: Option<Vec<String>>,

    #[clap(flatten)]
    bonus_specifier: BonusSpecifierArgs,

    // Specifies how the terminal output will be formatted
    #[clap(long, value_enum)]
    format: Option<PayoutListFormat>,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
pub struct PayoutSpecifierArgs {
    /// Pays out helpers at a fixed rate of X stardust per ticket
    #[clap(long)]
    stardust_rate: Option<f64>,
    /// Pays out helpers based on a stardust pool of X stardust, distributed proportionally to the number of tickets closed
    #[clap(long)]
    stardust_pool: Option<i32>,
}

#[derive(Debug, clap::Args)]
#[group(required = false, multiple = false)]
pub struct BonusSpecifierArgs {
    /// Number of stardust to give to bonus users, on top of normal payout
    #[clap(long)]
    bonus_stardust: Option<f64>,

    /// A higher-than-default stardust/ticket value to use for bonus users
    #[clap(long)]
    bonus_rate: Option<f64>,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum PayoutListFormat {
    /// Format the payout list in a way that's optimised for letting a
    /// Flavortown admin easily and accurately give the payouts manually
    #[clap(name = "payout")]
    ManualPayouts,
    /// Format the payout list in a way that makes sense for a
    /// Slack message
    #[clap(name = "message")]
    SlackMessage,
}

/// Payout calculation configuration, parsed from cli args into a struct that's easier to work with.
/// Configures how we'll calculate payouts, and what the rates will be.
enum PayoutCalcConfig {
    stardustPerTicket {
        stardust_rate: f64,
        bonus: Option<BonusConfig>,
    },
    Pool {
        pool: i32,
    },
}

/// Reward a subset of helpers with a bonus (e.g. increased multiplier, or a flat number of extra stardust)
struct BonusConfig {
    users: Vec<String>,
    bonus: BonusConfigBonus,
}

enum BonusConfigBonus {
    /// An extra number of stardust to give to users who deserve a bonus payout, on top of their normal payout.
    Extrastardust(f64),
    /// The stardust/ticket value to use for users who deserve a bonus payout.
    /// For this to make any sense, it should be greater than `--stardust-rate`
    stardustRate(f64),
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
    let nephthys_db_url =
        std::env::var("NEPHTHYS_DB_URL").context("NEPHTHYS_DB_URL environment variable not set")?;
    let args = CrimsonArgs::parse();
    let command_args: &PayoutArgs = match &args.command {
        Command::Payout(p) => p,
    };
    let start = parse_datetime(&command_args.start)?;
    let end = parse_datetime(&command_args.end)?;

    // Create payout config from command line args
    let payout_config = if let Some(stardust_rate) = &command_args.payout_specifier.stardust_rate {
        PayoutCalcConfig::stardustPerTicket {
            stardust_rate: *stardust_rate,
            bonus: if let Some(bonus_users) = &command_args.bonus_users {
                Some(BonusConfig {
                    users: bonus_users.clone(),
                    bonus: if let Some(rate) = command_args.bonus_specifier.bonus_rate {
                        BonusConfigBonus::stardustRate(rate)
                    } else if let Some(stardust) = command_args.bonus_specifier.bonus_stardust {
                        BonusConfigBonus::Extrastardust(stardust)
                    } else {
                        unreachable!("bonus_users specified without a valid bonus specifier")
                    },
                })
            } else {
                None // No bonus_users specified
            },
        }
    } else if let Some(pool) = &command_args.payout_specifier.stardust_pool {
        PayoutCalcConfig::Pool { pool: *pool }
    } else {
        unreachable!("One of stardust_rate or stardust_pool should be set")
    };

    let pretty_printer = format_description!(
        "[weekday] [day padding:none] [month repr:short] [year] (@ [hour]:[minute])"
    );
    println!(
        "Selecting leaderboard from {} to {} (Period: {})",
        start.format(&pretty_printer)?,
        end.format(&pretty_printer)?,
        end - start
    );
    if let Some(bonus_users) = &command_args.bonus_users {
        println!(
            "Giving bonus payouts to {} user(s): {}",
            bonus_users.len(),
            bonus_users.join(", ")
        );
    } else {
        println!("No bonus payouts");
    }

    let nephthys_db = Client::connect(&nephthys_db_url, NoTls)
        .context("Failed to connect to Nephthys database")?;

    let helper_tickets = get_helper_leaderboard(nephthys_db, start, end)?;

    let helper_stardust = calculate_payouts(&helper_tickets, &payout_config)?;

    print_helper_stardust(
        &helper_stardust,
        &helper_tickets,
        &command_args
            .clone()
            .format
            .unwrap_or(PayoutListFormat::ManualPayouts),
    )?;

    Ok(())
}

fn calculate_payouts(
    helper_tickets: &HashMap<String, i64>,
    payout_config: &PayoutCalcConfig,
) -> Result<HashMap<String, f64>, anyhow::Error> {
    match payout_config {
        PayoutCalcConfig::Pool { pool } => {
            let total_tickets_closed: i64 = helper_tickets.values().sum();
            let helper_stardust: HashMap<String, f64> = helper_tickets
                .iter()
                .map(|(id, tickets)| {
                    let payout = (*tickets as f64 / total_tickets_closed as f64) * (*pool as f64);
                    (id.clone(), payout)
                })
                .collect();
            Ok(helper_stardust)
        }
        PayoutCalcConfig::stardustPerTicket {
            stardust_rate: base_rate,
            bonus,
        } => match bonus {
            Some(bonus_config) => {
                let helper_stardust: HashMap<String, f64> = helper_tickets
                    .iter()
                    .map(|(id, tickets)| {
                        let tickets = *tickets as f64;
                        let payout = if bonus_config.users.contains(id) {
                            match &bonus_config.bonus {
                                BonusConfigBonus::Extrastardust(extra) => {
                                    (tickets * base_rate) + extra
                                }
                                BonusConfigBonus::stardustRate(bonus_rate) => tickets * bonus_rate,
                            }
                        } else {
                            tickets * base_rate
                        };
                        (id.clone(), payout)
                    })
                    .collect();
                Ok(helper_stardust)
            }
            None => {
                let helper_stardust: HashMap<String, f64> = helper_tickets
                    .iter()
                    .map(|(id, tickets)| (id.clone(), (*tickets as f64) * base_rate))
                    .collect();
                Ok(helper_stardust)
            }
        },
    }
}

fn print_helper_stardust(
    helper_stardust: &HashMap<String, f64>,
    helper_tickets: &HashMap<String, i64>,
    format: &PayoutListFormat,
) -> Result<(), anyhow::Error> {
    println!(
        "Total tickets closed: {}",
        helper_tickets.values().sum::<i64>()
    );
    println!(
        "Total stardust to pay out: {}",
        helper_stardust.values().sum::<f64>()
    );
    println!();

    let mut helper_stardust_vec: Vec<(&String, &f64)> = helper_stardust.iter().collect();
    helper_stardust_vec.sort_by(|(_, stardust_a), (_, stardust_b)| {
        stardust_b
            .partial_cmp(stardust_a)
            .expect("unexpected unorderable float")
    });
    for (slack_id, stardust) in helper_stardust_vec {
        match format {
            PayoutListFormat::ManualPayouts => println!(
                "{} gets {} stardust! ({} tkts)\n",
                slack_id,
                (*stardust as f32), // use f32 to reduce the chances of .0000000000001
                match helper_tickets.get(slack_id) {
                    Some(tickets) => tickets.to_string(),
                    None => "[unknown]".to_string(),
                },
            ),
            PayoutListFormat::SlackMessage => println!(
                "- *{}* closed *{}* tickets, netting them *{}* stardust.",
                slack_id,
                match helper_tickets.get(slack_id) {
                    Some(tickets) => tickets.to_string(),
                    None => "[unknown]".to_string(),
                },
                (*stardust).round()
            ),
        };
    }
    Ok(())
}

/// Returns a map of Slack IDs to tickets closed
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
