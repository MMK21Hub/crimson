# Crimson

A Rust tool to calculate cookie payouts for the [Flavortown](https://flavortown.hackclub.com/) support team.

Named _Crimson_ because we want to avoid any _Corruption_ when giving payouts.

## Usage

Fill out `.env` file:

```env
# Nephthys database details
DATABASE_URL="postgresql://username:password@coolify/nephthys"
# Flavortown API details (read-only access for now)
FLAVORTOWN_API_BASE="https://flavortown.hackclub.com"
FLAVORTOWN_API_KEY="ft_sk_aaaaaaaaaaaaaaa"
```

Run it, e.g.

```bash
cargo run payout --start "2026-02-02T20:00Z" --end "2026-02-06T00:00Z"
```
