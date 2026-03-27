"""
Crisis Pulse — Multi-Source Data Collector v2
==============================================
Sources:
  1. Reddit          — per-signal post volume from relevant subreddits (replaces Wikipedia)
  2. Google RSS      — per-market trending topics, classified into crisis/sport buckets
  3. NewsAPI         — per-signal article count, geo-filtered per market (fixed)
  4. Guardian        — per-signal article count, geo-filtered per market (fixed)
  5. Twitch          — global live gaming viewership

All Reddit calls use public .json endpoints — no API key, no OAuth required.

Writes:
  public/pulse_data.json    — rolling 7-day detail
  public/pulse_history.json — daily snapshot, appended + backfilled 30 days
"""

import os, json, time, logging, random, requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

MARKETS = {"AE": "UAE", "SA": "KSA", "KW": "Kuwait", "QA": "Qatar"}

# NewsAPI + Guardian geo-filter terms per market
MARKET_GEO_TERMS = {
    "UAE":    'UAE OR Dubai OR "Abu Dhabi" OR Emirates',
    "KSA":    '"Saudi Arabia" OR Riyadh OR Jeddah OR KSA',
    "Kuwait": "Kuwait",
    "Qatar":  "Qatar OR Doha",
}

SPORT_KW  = ["football","soccer","game","match","vs","ucl","league","cup","sport",
             "film","movie","music","cricket","ipl","nba","f1","basketball"]
CRISIS_KW = ["war","attack","crisis","shortage","price","inflation","ban",
             "sanction","protest","arrest","flood","earthquake","strike","conflict"]

BACKFILL_DAYS = 30

BASE_PATH    = Path(__file__).parent.parent
OUTPUT_PATH  = BASE_PATH / "public" / "pulse_data.json"
HISTORY_PATH = BASE_PATH / "public" / "pulse_history.json"
CONFIG_PATH  = BASE_PATH / "public" / "signals_config.json"

REDDIT_HEADERS = {
    "User-Agent": "CrisisPulse/2.0 (github-actions daily aggregator)",
}


# ── Config loader ─────────────────────────────────────────────────────────────

def load_config() -> dict:
    try:
        return json.loads(CONFIG_PATH.read_text())
    except Exception as e:
        log.error(f"Cannot load signals_config.json: {e}")
        raise

def flat_signals(config: dict) -> dict:
    """Returns { signal_key: { ...cfg, category, category_label, color, icon } }"""
    out = {}
    now = datetime.now(timezone.utc).date()
    ramadan_active = config.get("ramadan_active", False)
    ramadan_end    = config.get("ramadan_end", "")
    if ramadan_end:
        try:
            ramadan_active = ramadan_active and now <= datetime.fromisoformat(ramadan_end).date()
        except:
            pass
    for cat_key, cat in config["categories"].items():
        if cat.get("ramadan_only") and not ramadan_active:
            log.info(f"  Skipping Ramadan category (not active)")
            continue
        for sig_key, sig in cat["signals"].items():
            out[sig_key] = {**sig, "category": cat_key, "category_label": cat["label"],
                            "color": cat["color"], "icon": cat["icon"]}
    return out


# ── File helpers ──────────────────────────────────────────────────────────────

def safe_get(url, **kwargs):
    try:
        return requests.get(url, timeout=12, **kwargs)
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return None

def load_existing() -> dict:
    try:
        if OUTPUT_PATH.exists():
            d = json.loads(OUTPUT_PATH.read_text())
            if d.get("categories"):
                log.info(f"Loaded previous data ({d.get('fetched_at','?')})")
                return d
    except Exception as e:
        log.warning(f"Could not load existing: {e}")
    return {}

def load_history() -> list:
    try:
        if HISTORY_PATH.exists():
            return json.loads(HISTORY_PATH.read_text())
    except:
        pass
    return []

def save_history(h: list):
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(h, indent=2))


# ── Source 1: Reddit public JSON ──────────────────────────────────────────────
#
# Uses reddit.com's public .json endpoints — no API key, no OAuth.
# For each signal we search its configured subreddits using the signal's
# keyword query, counting posts from the last N days.
# Results are normalised 0-100 relative to the max across all signals.

def fetch_reddit_signal(subreddits: list, query: str, days: int = 1) -> int:
    """
    Count posts matching `query` across `subreddits` in the last `days` days.
    Uses Reddit search.json — no auth required.
    """
    cutoff    = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_ts = cutoff.timestamp()
    total     = 0

    for sub in subreddits[:3]:  # cap at 3 subs to avoid rate limits
        url    = f"https://www.reddit.com/r/{sub}/search.json"
        params = {
            "q": query, "sort": "new",
            "t": "week" if days <= 7 else "month",
            "limit": 100, "restrict_sr": 1,
        }
        r = safe_get(url, headers=REDDIT_HEADERS, params=params)
        if not r or r.status_code != 200:
            # Fallback: /new.json + keyword filter
            r2 = safe_get(f"https://www.reddit.com/r/{sub}/new.json",
                          headers=REDDIT_HEADERS, params={"limit": 100})
            if not r2 or r2.status_code != 200:
                time.sleep(0.6)
                continue
            try:
                posts = r2.json().get("data", {}).get("children", [])
            except:
                time.sleep(0.6)
                continue
            kws = query.lower().split()
            for p in posts:
                d = p.get("data", {})
                if d.get("created_utc", 0) < cutoff_ts:
                    continue
                text = (d.get("title", "") + " " + d.get("selftext", "")).lower()
                if any(kw in text for kw in kws):
                    total += 1
            time.sleep(0.6)
            continue

        try:
            posts = r.json().get("data", {}).get("children", [])
        except:
            time.sleep(0.6)
            continue

        for p in posts:
            if p.get("data", {}).get("created_utc", 0) >= cutoff_ts:
                total += 1

        time.sleep(0.6)

    return total


def fetch_reddit_all_signals(signals: dict, days: int = 1) -> dict:
    """
    Returns { sig_key: normalised_score_0_to_100 } for today.
    """
    raw: dict[str, int] = {}
    log.info(f"\nReddit ({days}-day window)...")
    for sig_key, cfg in signals.items():
        subs  = cfg.get("reddit_subs", ["all"])
        query = cfg.get("reddit_query") or cfg.get("news", sig_key)
        count = fetch_reddit_signal(subs, query, days=days)
        raw[sig_key] = count
        log.info(f"  {'OK' if count else '--'} {sig_key}: {count} posts")
        time.sleep(0.3)

    max_val = max(raw.values(), default=1) or 1
    return {k: round(v / max_val * 100, 1) for k, v in raw.items()}


def fetch_reddit_range(signals: dict, days: int = 30) -> dict:
    """
    Backfill: returns { sig_key: { 'YYYYMMDD': normalised_score } }.
    Fetches weekly buckets and distributes evenly across days.
    """
    log.info(f"\nReddit backfill ({days} days)...")
    now    = datetime.now(timezone.utc)
    result = {sig: {} for sig in signals}
    weeks  = (days // 7) + 1

    for sig_key, cfg in signals.items():
        subs  = cfg.get("reddit_subs", ["all"])
        query = cfg.get("reddit_query") or cfg.get("news", sig_key)
        for w in range(weeks):
            w_days = min(7, days - w * 7)
            if w_days <= 0:
                break
            count   = fetch_reddit_signal(subs, query, days=w_days + w * 7)
            per_day = round(count / max(w_days, 1))
            for d in range(w_days):
                day = now - timedelta(days=w * 7 + d + 1)
                result[sig_key][day.strftime("%Y%m%d")] = per_day
            time.sleep(0.5)

    all_vals = [v for sd in result.values() for v in sd.values()]
    max_val  = max(all_vals, default=1) or 1
    for sig_key in result:
        result[sig_key] = {k: round(v / max_val * 100, 1) for k, v in result[sig_key].items()}

    log.info(f"  Reddit backfill done — {len(result)} signals")
    return result


# ── Source 2: Google RSS ──────────────────────────────────────────────────────

def fetch_rss(geo: str) -> dict:
    r = safe_get(f"https://trends.google.com/trending/rss?geo={geo}",
                 headers={"User-Agent": "Mozilla/5.0"})
    if not r or r.status_code != 200:
        return {}
    try:
        root   = ET.fromstring(r.text)
        topics = [i.find("title").text or "" for i in root.findall(".//item") if i.find("title") is not None]
        total  = len(topics) or 1
        return {
            "sport_entertainment_pct": round(sum(1 for t in topics if any(k in t.lower() for k in SPORT_KW)) / total * 100),
            "crisis_pct":              round(sum(1 for t in topics if any(k in t.lower() for k in CRISIS_KW)) / total * 100),
            "top_topics":              topics[:10],
        }
    except Exception as e:
        log.warning(f"  RSS parse error [{geo}]: {e}")
        return {}


# ── Source 3: NewsAPI (per-signal, per-market) ────────────────────────────────

def fetch_newsapi_signal(query: str, market: str, from_date: str, api_key: str) -> int:
    if not api_key:
        return 0
    geo      = MARKET_GEO_TERMS.get(market, market)
    combined = f"({query}) AND ({geo})"
    r = safe_get("https://newsapi.org/v2/everything",
                 params={"q": combined, "from": from_date, "language": "en",
                         "pageSize": 1, "apiKey": api_key})
    if r and r.status_code == 200:
        return r.json().get("totalResults", 0)
    log.warning(f"  NewsAPI {r.status_code if r else 'timeout'} [{market}/{query[:25]}]")
    return 0


def fetch_newsapi_all(signals: dict, from_date: str, api_key: str) -> dict:
    """Returns { market: { sig_key: count } }"""
    if not api_key:
        log.warning("  NewsAPI key not set")
        return {}
    log.info("\nNewsAPI (per-signal, per-market)...")
    result = {m: {} for m in MARKETS.values()}
    ok = False
    for market_name in MARKETS.values():
        for sig_key, cfg in signals.items():
            count = fetch_newsapi_signal(cfg.get("news", sig_key), market_name, from_date, api_key)
            result[market_name][sig_key] = count
            if count:
                ok = True
            time.sleep(0.25)
        log.info(f"  OK {market_name}: {sum(result[market_name].values())} articles")
    return result if ok else {}


# ── Source 4: Guardian (per-signal, per-market) ───────────────────────────────

def fetch_guardian_signal(query: str, market: str, from_date: str, api_key: str,
                           to_date: str = "") -> int:
    if not api_key:
        return 0
    geo      = MARKET_GEO_TERMS.get(market, market)
    combined = f"{query} {geo}"
    params   = {"q": combined, "from-date": from_date, "api-key": api_key, "page-size": 1}
    if to_date:
        params["to-date"] = to_date
    r = safe_get("https://content.guardianapis.com/search", params=params)
    return r.json().get("response", {}).get("total", 0) if r and r.status_code == 200 else 0


def fetch_guardian_all(signals: dict, from_date: str, api_key: str) -> dict:
    """Returns { market: { sig_key: count } }"""
    if not api_key:
        log.warning("  Guardian key not set")
        return {}
    log.info("\nGuardian (per-signal, per-market)...")
    result = {m: {} for m in MARKETS.values()}
    ok = False
    for market_name in MARKETS.values():
        for sig_key, cfg in signals.items():
            count = fetch_guardian_signal(cfg.get("guardian", sig_key), market_name, from_date, api_key)
            result[market_name][sig_key] = count
            if count:
                ok = True
            time.sleep(0.3)
        log.info(f"  OK {market_name}: {sum(result[market_name].values())} articles")
    return result if ok else {}


# ── Source 5: Twitch ──────────────────────────────────────────────────────────

def fetch_twitch(client_id: str, client_secret: str) -> dict:
    if not client_id or not client_secret:
        return {}
    try:
        t = requests.post("https://id.twitch.tv/oauth2/token",
                          data={"client_id": client_id, "client_secret": client_secret,
                                "grant_type": "client_credentials"}, timeout=10)
        if t.status_code != 200:
            return {}
        token   = t.json()["access_token"]
        s       = requests.get("https://api.twitch.tv/helix/streams", params={"first": 20},
                               headers={"Client-Id": client_id, "Authorization": f"Bearer {token}"}, timeout=10)
        streams = s.json().get("data", []) if s.status_code == 200 else []
        games: dict[str, int] = {}
        for st in streams:
            g = st.get("game_name", "Unknown")
            games[g] = games.get(g, 0) + st["viewer_count"]
        top = sorted(games.items(), key=lambda x: x[1], reverse=True)[:5]
        return {
            "total_viewers": sum(st["viewer_count"] for st in streams),
            "top_games":     [{"name": g, "viewers": v} for g, v in top],
        }
    except Exception as e:
        log.warning(f"  Twitch error: {e}")
        return {}


# ── Blend RSS trends into signal scores ───────────────────────────────────────

CRISIS_SIGNALS = {"breaking_news", "crisis_topics", "war_news", "fact_checking"}
SPORT_SIGNALS  = {"gaming", "streaming", "humour"}

def blend_rss_into_scores(scores: dict, rss_data: dict, signals: dict) -> dict:
    """
    Nudge signal scores up based on per-market RSS percentages.
    Crisis signals are boosted when crisis_pct is high; escapism signals
    when sport_entertainment_pct is high. Caps at 100.
    """
    blended = {m: dict(sigs) for m, sigs in scores.items()}
    for market_name, rss in rss_data.items():
        crisis_pct = rss.get("crisis_pct", 0)
        sport_pct  = rss.get("sport_entertainment_pct", 0)
        for sig_key in signals:
            base = blended.get(market_name, {}).get(sig_key)
            if base is None:
                continue
            if sig_key in CRISIS_SIGNALS and crisis_pct > 15:
                blended[market_name][sig_key] = min(100, round(base + crisis_pct * 0.15, 1))
            elif sig_key in SPORT_SIGNALS and sport_pct > 15:
                blended[market_name][sig_key] = min(100, round(base + sport_pct * 0.10, 1))
    return blended


# ── Backfill ──────────────────────────────────────────────────────────────────

def backfill(signals: dict, guardian_key: str, newsapi_key: str) -> list:
    log.info(f"\nBackfilling {BACKFILL_DAYS} days...")
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=BACKFILL_DAYS)

    # Reddit: weekly buckets -> per-day
    reddit_range = fetch_reddit_range(signals, days=BACKFILL_DAYS)

    # Guardian: weekly buckets per signal per market
    guardian: dict = {m: {s: {} for s in signals} for m in MARKETS.values()}
    if guardian_key:
        for w in range(5):
            w_start = (start + timedelta(weeks=w)).strftime("%Y-%m-%d")
            w_end   = (start + timedelta(weeks=w + 1) - timedelta(days=1)).strftime("%Y-%m-%d")
            for market_name in MARKETS.values():
                for sig_key, cfg in signals.items():
                    count   = fetch_guardian_signal(cfg.get("guardian", sig_key), market_name,
                                                    w_start, guardian_key, w_end)
                    per_day = round(count / 7)
                    for d in range(7):
                        day = start + timedelta(weeks=w, days=d)
                        if day <= now:
                            guardian[market_name][sig_key][day.strftime("%Y%m%d")] = per_day
                    time.sleep(0.25)
        log.info(f"  Guardian backfill done")

    # NewsAPI: one query per signal per market for the full period
    newsapi: dict = {m: {} for m in MARKETS.values()}
    if newsapi_key:
        from_date = start.strftime("%Y-%m-%d")
        for market_name in MARKETS.values():
            for sig_key, cfg in signals.items():
                count   = fetch_newsapi_signal(cfg.get("news", sig_key), market_name, from_date, newsapi_key)
                per_day = max(1, round(count / BACKFILL_DAYS))
                newsapi[market_name][sig_key] = per_day
                time.sleep(0.25)
        log.info(f"  NewsAPI backfill done")

    # Assemble records
    records = []
    for days_ago in range(BACKFILL_DAYS, 0, -1):
        day     = now - timedelta(days=days_ago)
        day_str = day.strftime("%Y-%m-%d")
        day_key = day.strftime("%Y%m%d")
        record  = {"date": day_str, "markets": {}, "news_volumes": {}, "twitch_viewers": 0}

        for market_name in MARKETS.values():
            record["markets"][market_name] = {}
            for sig_key in signals:
                record["markets"][market_name][sig_key] = reddit_range.get(sig_key, {}).get(day_key)

        for sig_key in signals:
            g_avg = sum(guardian.get(m, {}).get(sig_key, {}).get(day_key, 0) for m in MARKETS.values()) / len(MARKETS)
            n_avg = sum(newsapi.get(m, {}).get(sig_key, 0) for m in MARKETS.values()) / len(MARKETS)
            record["news_volumes"][sig_key] = round(g_avg + n_avg)

        records.append(record)

    log.info(f"Backfill done — {len(records)} records")
    return records


# ── Today snapshot ────────────────────────────────────────────────────────────

def append_today(history: list, pulse: dict, signals: dict) -> list:
    today        = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history      = [r for r in history if r.get("date") != today]
    newsapi_raw  = pulse.get("news_volumes", {}).get("newsapi", {})
    guardian_raw = pulse.get("news_volumes", {}).get("guardian", {})
    reddit_raw   = pulse.get("global", {}).get("reddit", {})

    snap = {
        "date": today, "markets": {}, "news_volumes": {},
        "news_volumes_by_market": {},
        "twitch_viewers": pulse.get("global", {}).get("twitch", {}).get("total_viewers", 0),
    }

    for market_name in MARKETS.values():
        snap["markets"][market_name] = {}
        snap["news_volumes_by_market"][market_name] = {}
        for sig_key in signals:
            reddit_score = reddit_raw.get(sig_key)
            na           = newsapi_raw.get(market_name, {}).get(sig_key, 0)
            gd           = guardian_raw.get(market_name, {}).get(sig_key, 0)
            news_sum     = na + gd
            snap["news_volumes_by_market"][market_name][sig_key] = news_sum
            if reddit_score is not None:
                sig_max  = max(
                    (newsapi_raw.get(m, {}).get(sig_key, 0) + guardian_raw.get(m, {}).get(sig_key, 0))
                    for m in MARKETS.values()
                ) or 1
                news_norm = min(99, round((news_sum / sig_max) * 99))
                snap["markets"][market_name][sig_key] = round(reddit_score * 0.6 + news_norm * 0.4, 1)
            else:
                snap["markets"][market_name][sig_key] = None

    for sig_key in signals:
        na_g = sum(newsapi_raw.get(m, {}).get(sig_key, 0) for m in MARKETS.values())
        gd_g = sum(guardian_raw.get(m, {}).get(sig_key, 0) for m in MARKETS.values())
        snap["news_volumes"][sig_key] = na_g + gd_g

    history.append(snap)
    log.info(f"Appended {today}. Total: {len(history)} records")
    return history


# ── Main ──────────────────────────────────────────────────────────────────────

def collect():
    newsapi_key   = os.getenv("NEWSAPI_KEY", "")
    guardian_key  = os.getenv("GUARDIAN_KEY", "")
    twitch_id     = os.getenv("TWITCH_CLIENT_ID", "")
    twitch_secret = os.getenv("TWITCH_CLIENT_SECRET", "")

    config  = load_config()
    signals = flat_signals(config)
    log.info(f"{len(signals)} active signals across {len(config['categories'])} categories")

    existing  = load_existing()
    history   = load_history()
    now       = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    date_labels = [(now - timedelta(days=7 - i)).strftime("%b %d") for i in range(8)]

    # Backfill check
    existing_dates = {r["date"] for r in history}
    missing = [(now - timedelta(days=d)).strftime("%Y-%m-%d")
               for d in range(BACKFILL_DAYS, 0, -1)
               if (now - timedelta(days=d)).strftime("%Y-%m-%d") not in existing_dates]
    if missing:
        log.info(f"History missing {len(missing)} days — running backfill")
        backfilled = backfill(signals, guardian_key, newsapi_key)
        bf_by_date = {r["date"]: r for r in backfilled}
        history    = [r for r in history if r["date"] not in bf_by_date]
        history    = sorted(history + list(bf_by_date.values()), key=lambda r: r["date"])
        save_history(history)
        log.info(f"History: {len(history)} records after backfill")
    else:
        log.info(f"History complete — {len(history)} records")

    result = {
        "fetched_at":     now.isoformat(),
        "dates":          date_labels,
        "categories":     {},
        "markets":        {m: dict(existing.get("markets", {}).get(m, {})) for m in MARKETS.values()},
        "global":         {},
        "news_volumes":   {},
        "sources_live":   [],
        "sources_failed": [],
        "ramadan_active": config.get("ramadan_active", False),
    }
    for cat_key, cat in config["categories"].items():
        result["categories"][cat_key] = {
            "label": cat["label"], "icon": cat["icon"], "color": cat["color"],
            "hypothesis": cat.get("hypothesis", ""),
            "ramadan_only": cat.get("ramadan_only", False),
            "signals": list(cat["signals"].keys()),
        }

    # Reddit
    reddit_scores = fetch_reddit_all_signals(signals, days=1)
    if reddit_scores:
        result["global"]["reddit"] = reddit_scores
        result["sources_live"].append("reddit")
        for market_name in MARKETS.values():
            for sig_key, score in reddit_scores.items():
                if not result["markets"][market_name].get(sig_key):
                    result["markets"][market_name][sig_key] = score
    else:
        result["sources_failed"].append("reddit")

    # Google RSS
    log.info("\nGoogle RSS Trends...")
    rss_data = {}
    for geo, market_name in MARKETS.items():
        time.sleep(random.uniform(1, 2))
        trends = fetch_rss(geo)
        if trends:
            rss_data[market_name] = trends
            log.info(f"  OK {market_name}: sport={trends['sport_entertainment_pct']}% crisis={trends['crisis_pct']}%")
        else:
            log.warning(f"  -- {market_name}")
    if rss_data:
        result["global"]["rss_trends"] = rss_data
        result["sources_live"].append("google_rss")
        result["markets"] = blend_rss_into_scores(result["markets"], rss_data, signals)
    else:
        result["sources_failed"].append("google_rss")

    # NewsAPI
    newsapi_vols = fetch_newsapi_all(signals, from_date, newsapi_key)
    if newsapi_vols:
        result["news_volumes"]["newsapi"] = newsapi_vols
        result["news_volumes"]["newsapi_global"] = {
            sig: sum(newsapi_vols.get(m, {}).get(sig, 0) for m in MARKETS.values())
            for sig in signals
        }
        result["sources_live"].append("newsapi")
    else:
        result["sources_failed"].append("newsapi")

    # Guardian
    guardian_vols = fetch_guardian_all(signals, from_date, guardian_key)
    if guardian_vols:
        result["news_volumes"]["guardian"] = guardian_vols
        result["sources_live"].append("guardian")
    else:
        result["sources_failed"].append("guardian")

    # Twitch
    log.info("\nTwitch...")
    if twitch_id and twitch_secret:
        twitch_data = fetch_twitch(twitch_id, twitch_secret)
        if twitch_data:
            result["global"]["twitch"] = twitch_data
            result["sources_live"].append("twitch")
            log.info(f"  OK {twitch_data['total_viewers']:,} viewers")
        else:
            result["sources_failed"].append("twitch")
    else:
        result["sources_failed"].append("twitch")

    log.info(f"\nLive: {result['sources_live']}")
    if result["sources_failed"]:
        log.info(f"Failed: {result['sources_failed']}")

    return result, signals, config


# ── Market Summaries ──────────────────────────────────────────────────────────

def generate_market_summary(market: str, data: dict, config: dict) -> str:
    categories   = config.get("categories", {})
    newsapi_raw  = data.get("news_volumes", {}).get("newsapi", {})
    guardian_raw = data.get("news_volumes", {}).get("guardian", {})
    reddit_raw   = data.get("global", {}).get("reddit", {})
    rss          = data.get("global", {}).get("rss_trends", {}).get(market, {})
    is_ramadan   = bool(data.get("ramadan_active") and config.get("ramadan_active"))

    all_signals = {}
    for ck, cat in categories.items():
        if cat.get("ramadan_only") and not is_ramadan:
            continue
        for sk in cat.get("signals", {}).keys():
            na = newsapi_raw.get(market, {}).get(sk, 0)
            gd = guardian_raw.get(market, {}).get(sk, 0)
            rd = reddit_raw.get(sk, 0) if reddit_raw else 0
            all_signals[sk] = {"score": na + gd + rd, "cat": ck, "cat_label": cat["label"]}

    ranked      = sorted(all_signals.items(), key=lambda x: x[1]["score"], reverse=True)
    top2        = ranked[:2]
    cat_scores  = {}
    for sk, info in all_signals.items():
        cat_scores[info["cat"]] = cat_scores.get(info["cat"], 0) + info["score"]
    top_cat_key   = max(cat_scores, key=cat_scores.get) if cat_scores else ""
    top_cat_label = categories.get(top_cat_key, {}).get("label", "")
    sport_pct     = rss.get("sport_entertainment_pct", 0)
    crisis_pct    = rss.get("crisis_pct", 0)
    trend_str     = ", ".join(rss.get("top_topics", [])[:3]) or None

    def sl(sk): return sk.replace("_", " ")

    mood  = "crisis-driven" if crisis_pct > sport_pct else "entertainment-led"
    p1    = (f"Consumer attention in {market} is concentrated around **{sl(top2[0][0])}** and "
             f"**{sl(top2[1][0])}**, which together dominate the signal landscape. "
             f"The overall mood is {mood}, with {top_cat_label} emerging as the strongest category."
             + (" Ramadan is amplifying late-night activity and iftar-related consumption." if is_ramadan else ""))

    drivers = []
    if crisis_pct >= 20: drivers.append(f"elevated regional crisis coverage ({crisis_pct}% of trending topics)")
    if sport_pct  >= 20: drivers.append(f"strong sports and entertainment engagement ({sport_pct}%)")
    if is_ramadan:        drivers.append("the Ramadan consumption cycle shifting peak hours to evenings")
    if trend_str:         drivers.append(f"trending conversations around {trend_str}")
    if not drivers:       drivers.append("a mix of seasonal and regional factors")
    p2 = (f"This pattern is being driven by {'; '.join(drivers)}. "
          f"The {sl(top2[0][0])} signal in particular reflects the current media environment "
          f"across MENA, with audiences actively tracking developing stories alongside daily life.")

    if crisis_pct >= 20:
        action = (f"avoid hard promotional messaging this week — contextual and empathy-led "
                  f"creatives will perform better alongside {sl(top2[0][0])} content environments")
    elif is_ramadan:
        action = (f"activate Ramadan prime time (9-11pm) — iftar-moment sponsorships and "
                  f"evening digital placements capture peak {sl(top2[1][0])} engagement")
    else:
        action = (f"lean into {top_cat_label.lower()} environments — the {sl(top2[0][0])} signal "
                  f"suggests audiences are primed for discovery content over hard sell this week")
    p3 = f"For media planners in {market}: {action}."

    return f"{p1}\n\n{p2}\n\n{p3}"


def generate_all_summaries(data: dict, config: dict) -> dict:
    log.info("\nGenerating market summaries...")
    summaries = {}
    for market in MARKETS.values():
        try:
            text = generate_market_summary(market, data, config)
            summaries[market] = text
            log.info(f"  OK {market} ({len(text.split())} words)")
        except Exception as e:
            log.warning(f"  -- {market}: {e}")
    return summaries


# ── Entry ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("  Crisis Pulse — Multi-Source Collector v2")
    log.info(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 50)

    try:
        data, signals, config = collect()
    except Exception as e:
        log.error(f"Collection failed: {e}")
        existing = load_existing()
        if existing:
            existing["fetched_at"] = datetime.now(timezone.utc).isoformat()
            existing["error"]      = str(e)
            data, signals, config  = existing, {}, {}
        else:
            raise

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(data, indent=2))
    log.info(f"pulse_data.json written")

    if signals:
        history = load_history()
        history = append_today(history, data, signals)
        save_history(history)
        log.info(f"pulse_history.json written")
