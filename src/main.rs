use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::Url;

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

    /// End time (ISO 6801, e.g. 2026-03-01T00:00:00Z). Defaults to now.
    #[arg(long)]
    end: Option<String>,

    /// Pays out helpers at a fixed rate of X cookies per ticket
    #[clap(long)]
    cookie_rate: Option<f64>,
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

    Ok(())
}
