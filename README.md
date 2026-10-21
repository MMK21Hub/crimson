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

### Issuing payouts

```none
Usage: crimson payout [OPTIONS] --stardust-rate <STARDUST_RATE>

Options:
      --start <START>                  Start time (ISO 8601, e.g. 2026-02-01T00:00:00Z). Required on the first run
      --end <END>                      End time (ISO 8601, e.g. 2026-03-01T00:00:00Z). Defaults to now
      --stardust-rate <STARDUST_RATE>  Stardust paid out per closed ticket
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

## Credits

Made with <3 by Mish for [Hack Club](https://hackclub.com/).

Open-source under the [MIT License](LICENSE).
