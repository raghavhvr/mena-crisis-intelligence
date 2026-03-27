# Crisis Pulse

**Real-time consumer signal dashboard for MENA markets.**

Built for WPP MENA crisis reporting. Monitors behavioral and media signals across UAE, KSA, Kuwait, and Qatar — refreshed daily, zero infrastructure cost.

---

## Data Sources

| Source | What It Measures | Auth Required |
|---|---|---|
| **Reddit (public JSON)** | Per-signal post volume across curated subreddits, normalised 0–100 | None |
| **Google Trends RSS** | Trending search topics per market, classified crisis vs sport | None |
| **NewsAPI** | Per-signal article count geo-filtered per market over 7 days | Free API key |
| **The Guardian API** | Per-signal article count geo-filtered per market over 7 days | Free API key |
| **Twitch API** | Live global gaming viewership and top titles | Free app registration |

All sources are free tier. No paid APIs. No cloud server.

---

## What Was Fixed (v2)

| Issue | Fix |
|---|---|
| Wikipedia replaced | Reddit public `.json` endpoints — per-signal, no auth |
| NewsAPI was per-market (even split) | Now per-signal × per-market using each signal's `news` query + geo terms |
| Guardian had no geo-filter | Each query now appended with market geo terms (e.g. `UAE OR Dubai`) |
| Wikipedia same score for all markets | Reddit is global; market differentiation comes from NewsAPI + Guardian blending |
| RSS trends only used for text summaries | RSS crisis/sport percentages now nudge relevant signal scores numerically |
| Backfill was flat (same value every day) | Backfill now uses per-signal Reddit weekly buckets + per-signal Guardian/NewsAPI queries |

---

## Markets

UAE · KSA · Kuwait · Qatar

---

## Architecture

```
GitHub Actions (daily 09:00 GST)
    └── scripts/collect.py
            ├── Reddit public JSON (r/{sub}/search.json — no auth)
            ├── Google Trends RSS
            ├── NewsAPI  (per-signal × per-market queries)
            ├── Guardian (per-signal × per-market queries)
            └── Twitch API
                    ↓
            public/pulse_data.json   (committed to repo)
            public/pulse_history.json
                    ↓
            Vercel (auto-deploys on push)
                    ↓
            src/App.tsx  (reads /pulse_data.json at load)
```

No backend server. The collector runs on GitHub's infrastructure, writes static JSON files, and Vercel serves them.

---

## Signal Config

Each signal in `public/signals_config.json` now carries:

```json
{
  "label": "Gaming",
  "reddit_subs":  ["gaming", "pcgaming", "PS5"],
  "reddit_query": "gaming esports video games",
  "news":         "gaming esports video games",
  "guardian":     "gaming"
}
```

- `reddit_subs` — up to 3 subreddits to search (public `.json`, no auth)
- `reddit_query` — keyword string passed to Reddit search
- `news` — keyword string for NewsAPI (combined with market geo term)
- `guardian` — keyword string for Guardian (combined with market geo term)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/raghavhvr/crisis-pulse
cd crisis-pulse
```

### 2. Install frontend dependencies

```bash
npm install
npm run dev
```

### 3. Configure API keys

Create a `.env` file in the repo root (never committed):

```
NEWSAPI_KEY=your_key_here
GUARDIAN_KEY=your_key_here
TWITCH_CLIENT_ID=your_client_id_here
TWITCH_CLIENT_SECRET=your_client_secret_here
```

Reddit requires no key — it uses public `.json` endpoints.

Get free keys from:
- NewsAPI → [newsapi.org](https://newsapi.org)
- Guardian → [open-platform.theguardian.com](https://open-platform.theguardian.com)
- Twitch → [dev.twitch.tv/console](https://dev.twitch.tv/console)

### 4. Run the collector locally

```bash
pip install requests python-dotenv
python scripts/collect.py
```

This writes `public/pulse_data.json` and `public/pulse_history.json`.

### 5. Deploy

**Vercel** — connect the GitHub repo, set framework to Vite, deploy. Vercel auto-redeploys on every push.

**GitHub Actions** — add your four API keys as repository secrets (Settings → Secrets → Actions). The workflow runs daily at 05:00 UTC and commits fresh data automatically.

---

## GitHub Actions Secrets Required

| Secret | Source |
|---|---|
| `NEWSAPI_KEY` | newsapi.org |
| `GUARDIAN_KEY` | open-platform.theguardian.com |
| `TWITCH_CLIENT_ID` | dev.twitch.tv |
| `TWITCH_CLIENT_SECRET` | dev.twitch.tv |

Reddit needs no secret — it's a public API.

---

## Rate Limits

| Source | Limit | How we stay within it |
|---|---|---|
| Reddit public JSON | ~1 req/sec per IP | 0.6s sleep between subreddit calls |
| NewsAPI free | 100 req/day | 0.25s sleep; ~4 markets × 32 signals = 128 calls (backfill only) |
| Guardian free | 500 req/day | 0.3s sleep; same budget as NewsAPI |
| Google Trends RSS | Unofficial, no published limit | 1–2s random sleep between markets |
| Twitch | Token-based, generous | Single call per run |

On a normal daily run (no backfill), NewsAPI and Guardian make `4 markets × 32 signals = 128 calls each`. This fits within free tier for Guardian (500/day) but bumps NewsAPI's free tier (100/day). If this is an issue, reduce to the 2 highest-signal markets or upgrade to a paid NewsAPI plan.

---

## Local Development

```bash
npm run dev                  # start Vite dev server
python scripts/collect.py    # refresh data manually
```

---

## Requirements

**Frontend** — React 18, Vite, Recharts, TypeScript

**Collector** — Python 3.11+, `requests`, `python-dotenv`

---

## License

Internal tool — WPP MENA. Not for public redistribution.
