# Crimson

A Rust tool to calculate, track, and issue stardust payouts for the [Stardance](https://stardance.hackclub.com/) Support Scouts.

Named _Crimson_ because we want to avoid any _Corruption_ when giving payouts.

## Usage

Fill out `.env` file:

```env
# Nephthys database details
NEPHTHYS_DB_URL='postgresql://username:password@coolify/nephthys'
# Stardance API details
STARDANCE_API_KEY='sd_sk_aaaaaaaaaaaaaaaa'
```

### Fetching helper info

This is technically optional, but I recommend running `refresh-helpers` before anything else, to pull helper' user info from Stardance into the local database.

```bash
cargo run refresh-helpers
```

Otherwise, you will see "Stardance ID unknown" during dry runs.

### Issuing payouts

```none
Usage: crimson payout [OPTIONS] --stardust-rate <STARDUST_RATE>

Options:
      --start <START>                  Start time (ISO 8601, e.g. 2026-02-01T00:00:00Z). Required on the first regular run and for bonus payouts
      --end <END>                      End time (ISO 8601, e.g. 2026-03-01T00:00:00Z). Defaults to now for regular payouts; required for bonus payouts
      --stardust-rate <STARDUST_RATE>  Stardust paid out per closed ticket
      --bonus                          Make this a one-off bonus payout (requires --start, --end and --message)
      --message <MESSAGE>              Memo for a bonus payout (required with --bonus)
      --execute                        Actually send payouts. Without this flag the command only reports what it would do
  -h, --help                           Print help
```

#### Initial run

If this is the first payout session, manually specify a start date.

```bash
cargo run -- payout --start "2030-01-10T00:00Z" --stardust-rate 0.3
```

Crimson always performs a dry run by default, so after checking the payouts look sensible, add `--execute` to actually issue stardust.

Crimson will also store the payout session info in its database, so the next payout's time range will start at the end of the previous payout's time range.

#### Regular runs

By default, this will pay out all for tickets closed since the last payout session, and then log it in the database.

```bash
cargo run -- payout --stardust-rate 0.4
```

#### Bonus payouts

One-off bonuses (e.g. for a lock-in huddle) use `--bonus`, an explicit ticket period, and a short memo. The memo is stored with the session and appended to the Stardance reason in quotes.

```bash
cargo run -- payout --bonus \
  --start "2026-10-06T17:00Z" --end "2026-10-06T22:00Z" \
  --stardust-rate 0.2 --message "Tuesday lock-in huddle"
```

Bonus sessions are recorded like regular ones, but are ignored when calculating the next regular payout's start date, so they can overlap regular periods. Add `--execute` to actually send one.

#### Excluding people from payouts

People who are paid to work on Stardance are excluded from payouts, since they get paid Actual Money&trade; to answer questions. To accomplish this, set `excluded` to `1` in the database for all users who should be excluded. This has to be done after the [users table is initialised](#fetching-helper-info).

As of October 2026, the list of staff members to exclude should be this:

```sql
UPDATE user
SET excluded = 1
WHERE slack_id IN ('U08TCSANHDX','U09MY3SS6CV','U06PR6B8D37','U054VC2KM9P','U0AQSGVUUS3','U05F4B48GBF','U091PP9SN02','U0926UASBJ7','U0943NG73PA','U05PRRU5GSJ','U07BN55GN3D','U08R4KDL6UF','U07DJMFAQQP','U073M5L9U13','U09UQ385LSG','U09UE480JHH');
```

## Credits

Made with <3 by Mish for [Hack Club](https://hackclub.com/).

Open-source under the [MIT License](LICENSE).
