use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use time::macros::datetime;
use time::{OffsetDateTime, PrimitiveDateTime};

fn main() -> anyhow::Result<()> {
    // Env var config
    dotenvy::dotenv().ok();
    let db_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

    let client = Client::connect(&db_url, NoTls)?;

    let x = get_helper_leaderboard(
        client,
        datetime!(2026-02-02 20:00:00 UTC),
        datetime!(2026-02-06 00:00:00 UTC),
    )?;

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
