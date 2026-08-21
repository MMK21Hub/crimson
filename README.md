# Crimson

A Rust tool to calculate stardust payouts for the [Stardance](https://stardance.hackclub.com/) Support Scouts.

Named _Crimson_ because we want to avoid any _Corruption_ when giving payouts.

## Usage

Fill out `.env` file:

```env
# Nephthys database details
DATABASE_URL="postgresql://username:password@coolify/nephthys"
```

Run it, e.g.

```bash
cargo run payout --start "2026-02-02T20:00Z" --end "2026-02-06T00:00Z" --stardust-rate 0.5
```

## Credits

Made with <3 by Mish for [Hack Club](https://hackclub.com/).

Open-source under the [MIT License](LICENSE).
