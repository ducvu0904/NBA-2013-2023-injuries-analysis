"""
fetch_nba_stats.py
==================
Fetch toàn bộ player stats từ NBA API (12 endpoints × 10 seasons)
rồi merge vào injury dataset.

Cài đặt:
    pip install nba_api pandas

Chạy:
    python fetch_nba_stats.py

Output:
    nba_stats_cache/          -- raw data từng endpoint/season (tránh fetch lại)
    injury_stat_enriched.csv  -- dataset gốc + ~80 features mới
"""

import time
import os
import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashptstats

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE  = "injury_stat_filled.csv"   # output từ fill_team_fast.py
OUTPUT_FILE = "injury_stat_enriched.csv"
CACHE_DIR   = "nba_stats_cache"
DELAY       = 1.5  # giây giữa các API calls

SEASONS = [
    "2013-14","2014-15","2015-16","2016-17","2017-18",
    "2018-19","2019-20","2020-21","2021-22","2022-23",
]

# Convert season format: "2022-23" <-> "22-23"
def to_short(s): return s[2:]      # "2022-23" → "22-23"
def to_long(s):                    # "22-23"   → "2022-23"
    y = int(s[:2])
    suffix = s[3:]
    return f"20{y:02d}-{suffix}"


# ── Endpoint definitions ──────────────────────────────────────────────────────
#
# Mỗi entry: (cache_key, fetch_fn, columns_to_keep)
# fetch_fn nhận season string dạng "2022-23", trả về DataFrame
#
# Columns bị drop: PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, NICKNAME, GP, W, L,
#                  W_PCT, MIN — đã có trong injury dataset hoặc không cần

def fetch_traditional(season):
    r = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season, per_mode_detailed="PerGame",
        measure_type_detailed_defense="Base", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","PTS","FGM","FGA","FG_PCT","FG3M","FG3A","FG3_PCT",
            "FTM","FTA","FT_PCT","OREB","DREB","REB","AST","STL","BLK","TOV",
            "PF","PLUS_MINUS"]
    return df[keep]

def fetch_advanced(season):
    r = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season, per_mode_detailed="PerGame",
        measure_type_detailed_defense="Advanced", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","OFF_RATING","DEF_RATING","NET_RATING","AST_PCT",
            "AST_TO","AST_RATIO","OREB_PCT","DREB_PCT","REB_PCT",
            "TM_TOV_PCT","EFG_PCT","TS_PCT","PIE"]
    return df[[c for c in keep if c in df.columns]]

def fetch_scoring(season):
    r = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season, per_mode_detailed="PerGame",
        measure_type_detailed_defense="Scoring", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","PCT_FGA_2PT","PCT_FGA_3PT",
            "PCT_PTS_2PT","PCT_PTS_2PT_MR","PCT_PTS_3PT","PCT_PTS_FB",
            "PCT_PTS_FT","PCT_PTS_OFF_TOV","PCT_PTS_PAINT",
            "PCT_AST_2PM","PCT_UAST_2PM","PCT_AST_3PM","PCT_UAST_3PM",
            "PCT_AST_FGM","PCT_UAST_FGM"]
    return df[[c for c in keep if c in df.columns]]

def fetch_misc(season):
    r = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season, per_mode_detailed="PerGame",
        measure_type_detailed_defense="Misc", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","PTS_OFF_TOV","PTS_2ND_CHANCE","PTS_FB","PTS_PAINT",
            "OPP_PTS_OFF_TOV","OPP_PTS_2ND_CHANCE","OPP_PTS_FB","OPP_PTS_PAINT",
            "BLK_PCT","BLKA_PCT","FOULS_DRAWN"]
    return df[[c for c in keep if c in df.columns]]

def fetch_speed(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="SpeedDistance", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","DIST_MILES","DIST_MILES_OFF","DIST_MILES_DEF",
            "AVG_SPEED","AVG_SPEED_OFF","AVG_SPEED_DEF"]
    return df[[c for c in keep if c in df.columns]]

def fetch_drives(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="Drives", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","DRIVES","DRIVE_FGM","DRIVE_FGA","DRIVE_FG_PCT",
            "DRIVE_FTM","DRIVE_FTA","DRIVE_PTS","DRIVE_PTS_PCT",
            "DRIVE_PASSES","DRIVE_PASS_PCT","DRIVE_AST","DRIVE_TOV"]
    return df[[c for c in keep if c in df.columns]]

def fetch_touches(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="Possessions", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","TOUCHES","FRONT_CT_TOUCHES","TIME_OF_POSS",
            "AVG_SEC_PER_TOUCH","AVG_DRIB_PER_TOUCH","PTS_PER_TOUCH",
            "ELBOW_TOUCHES","POST_TOUCHES","PAINT_TOUCHES",
            "PTS_PER_ELBOW","PTS_PER_POST","PTS_PER_PAINT"]
    return df[[c for c in keep if c in df.columns]]

def fetch_catch_shoot(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="CatchShoot", timeout=30,
    )
    df = r.get_data_frames()[0]
    rename = {
        "FGM":"CS_FGM","FGA":"CS_FGA","FG_PCT":"CS_FG_PCT",
        "FG3M":"CS_3PM","FG3A":"CS_3PA","PTS":"CS_PTS","EFG_PCT":"CS_EFG_PCT",
    }
    df = df.rename(columns=rename)
    keep = ["PLAYER_ID","CS_FGM","CS_FGA","CS_FG_PCT",
            "CS_3PM","CS_3PA","CS_PTS","CS_EFG_PCT"]
    return df[[c for c in keep if c in df.columns]]

def fetch_pullup(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="PullUpShot", timeout=30,
    )
    df = r.get_data_frames()[0]
    rename = {
        "FGM":"PULL_UP_FGM","FGA":"PULL_UP_FGA","FG_PCT":"PULL_UP_FG_PCT",
        "FG3M":"PULL_UP_3PM","FG3A":"PULL_UP_FG3A","PTS":"PULL_UP_PTS",
        "EFG_PCT":"PULL_UP_EFG_PCT",
    }
    df = df.rename(columns=rename)
    keep = ["PLAYER_ID","PULL_UP_FGM","PULL_UP_FGA","PULL_UP_FG_PCT",
            "PULL_UP_3PM","PULL_UP_FG3A","PULL_UP_PTS","PULL_UP_EFG_PCT"]
    return df[[c for c in keep if c in df.columns]]

def fetch_passing(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="Passing", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","PASSES_MADE","PASSES_RECEIVED","SECONDARY_AST",
            "POTENTIAL_AST","AST_PTS_CREATED","AST_ADJ",
            "AST_TO_PASS_PCT","AST_TO_PASS_PCT_ADJ"]
    return df[[c for c in keep if c in df.columns]]

def fetch_rebounding(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="Rebounding", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","OREB_CHANCE_PCT","DREB_CHANCE_PCT","REB_CHANCE_PCT_ADJ",
            "C_OREB","C_DREB","UC_OREB","UC_DREB"]
    return df[[c for c in keep if c in df.columns]]

def fetch_defense(season):
    r = leaguedashptstats.LeagueDashPtStats(
        season=season, per_mode_simple="PerGame",
        player_or_team="Player",
        pt_measure_type="Defense", timeout=30,
    )
    df = r.get_data_frames()[0]
    keep = ["PLAYER_ID","DEF_AT_RIM_FGM","DEF_AT_RIM_FGA","DEF_AT_RIM_FG_PCT",
            "SCREEN_AST","SCREEN_AST_PTS","DEFLECTIONS",
            "LOOSE_BALLS_RECOVERED","CHARGES_DRAWN"]
    return df[[c for c in keep if c in df.columns]]


ENDPOINTS = [
    ("traditional",  fetch_traditional),
    ("advanced",     fetch_advanced),
    ("scoring",      fetch_scoring),
    ("misc",         fetch_misc),
    ("speed",        fetch_speed),
    ("drives",       fetch_drives),
    ("touches",      fetch_touches),
    ("catch_shoot",  fetch_catch_shoot),
    ("pullup",       fetch_pullup),
    ("passing",      fetch_passing),
    ("rebounding",   fetch_rebounding),
    ("defense",      fetch_defense),
]

PT_ENDPOINT_KEYS = {
    "speed", "drives", "touches", "catch_shoot",
    "pullup", "passing", "rebounding", "defense",
}


# ── Cache helpers ─────────────────────────────────────────────────────────────
def cache_path(key, season):
    return os.path.join(CACHE_DIR, f"{key}_{season.replace('-','_')}.csv")

def load_cache(key, season):
    p = cache_path(key, season)
    return pd.read_csv(p) if os.path.exists(p) else None

def save_cache(df, key, season):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.to_csv(cache_path(key, season), index=False)


def purge_invalid_pt_caches():
    """
    Xoa cache cũ của LeagueDashPtStats nếu thiếu PLAYER_ID/PERSON_ID
    (thường là cache team-level từ phiên bản script trước).
    """
    removed = 0
    checked = 0

    for ep_key in PT_ENDPOINT_KEYS:
        for season in SEASONS:
            p = cache_path(ep_key, season)
            if not os.path.exists(p):
                continue

            checked += 1
            try:
                cdf = pd.read_csv(p, nrows=5)
                cols = set(cdf.columns)
                if "PLAYER_ID" not in cols and "PERSON_ID" not in cols:
                    os.remove(p)
                    removed += 1
            except Exception:
                # Cache hỏng/không đọc được thì xoá để fetch lại sạch.
                os.remove(p)
                removed += 1

    if checked > 0:
        print(f"Cache cleanup: checked {checked} pt files, removed {removed} invalid files")


def normalize_stats_df(df, short_season, ep_key):
    """
    Chuẩn hóa DataFrame trả về từ endpoint/cache để merge an toàn.
    - Đảm bảo có PLAYER_ID (fallback từ PERSON_ID)
    - Đảm bảo có SEASON
    - Bỏ dòng PLAYER_ID null
    """
    if df is None or df.empty:
        return None

    out = df.copy()

    # Fallback cho một số endpoint có thể trả PERSON_ID thay vì PLAYER_ID
    if "PLAYER_ID" not in out.columns and "PERSON_ID" in out.columns:
        out = out.rename(columns={"PERSON_ID": "PLAYER_ID"})

    if "PLAYER_ID" not in out.columns:
        print(f"  ↳ WARN: skip {ep_key} {short_season} (missing PLAYER_ID)")
        return None

    if "SEASON" not in out.columns:
        out["SEASON"] = short_season

    out = out[out["PLAYER_ID"].notna()].copy()
    if out.empty:
        print(f"  ↳ WARN: skip {ep_key} {short_season} (no valid PLAYER_ID rows)")
        return None

    # Chuẩn hóa type join key để giảm lỗi merge do mixed type
    out["PLAYER_ID"] = pd.to_numeric(out["PLAYER_ID"], errors="coerce")
    out = out[out["PLAYER_ID"].notna()].copy()
    out["PLAYER_ID"] = out["PLAYER_ID"].astype("int64")
    out["SEASON"] = out["SEASON"].astype(str)

    return out


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_all_seasons():
    """
    Fetch tất cả endpoints × seasons.
    Trả về dict: {season_short: merged_DataFrame}
    """
    season_frames = {}   # "22-23" → merged df của season đó

    total = len(ENDPOINTS) * len(SEASONS)
    done  = 0

    for ep_key, fetch_fn in ENDPOINTS:
        ep_frames = []   # tất cả seasons cho endpoint này

        for season in SEASONS:
            done += 1
            short = to_short(season)

            # Check cache
            cached = load_cache(ep_key, season)
            if cached is not None:
                print(f"[{done:>3}/{total}] {ep_key:<15} {season}  (cached)")
                normalized = normalize_stats_df(cached, short, ep_key)
                if normalized is not None:
                    ep_frames.append((short, normalized))
                    continue
                print("  ↳ WARN: cached frame invalid, refetching...")

            # Fetch
            print(f"[{done:>3}/{total}] {ep_key:<15} {season}  fetching...", end=" ", flush=True)
            try:
                df = fetch_fn(season)
                df["SEASON"] = short
                save_cache(df, ep_key, season)
                normalized = normalize_stats_df(df, short, ep_key)
                if normalized is not None:
                    ep_frames.append((short, normalized))
                    print(f"→ {len(normalized)} players")
                else:
                    print("→ skipped (invalid frame)")
            except Exception as e:
                print(f"→ ERROR: {e}")

            time.sleep(DELAY)

        # Merge endpoint vào season_frames
        for short, df in ep_frames:
            if short not in season_frames:
                season_frames[short] = df
            else:
                # Merge trên PLAYER_ID + SEASON, tránh duplicate columns
                existing = season_frames[short]
                new_cols = [c for c in df.columns if c not in existing.columns or c in ("PLAYER_ID","SEASON")]
                season_frames[short] = existing.merge(
                    df[new_cols], on=["PLAYER_ID","SEASON"], how="left"
                )

    return season_frames


def main():
    # ── Load injury dataset ───────────────────────────────────────────────────
    injury = pd.read_csv(INPUT_FILE)
    print(f"Injury dataset: {len(injury)} rows, {len(injury.columns)} columns\n")

    # ── Cleanup old invalid pt caches (team-level) ───────────────────────────
    purge_invalid_pt_caches()

    # ── Fetch stats ───────────────────────────────────────────────────────────
    print(f"Fetching {len(ENDPOINTS)} endpoints × {len(SEASONS)} seasons "
          f"= {len(ENDPOINTS)*len(SEASONS)} calls (~{len(ENDPOINTS)*len(SEASONS)*DELAY/60:.1f} min)\n")

    season_frames = fetch_all_seasons()

    if not season_frames:
        raise RuntimeError("No season data was fetched successfully. Check endpoint errors/cache.")

    # Gộp tất cả seasons thành 1 DataFrame
    stats_all = pd.concat(list(season_frames.values()), ignore_index=True)
    print(f"\nStats DataFrame: {len(stats_all)} rows × {len(stats_all.columns)} columns")

    # ── Drop columns đã có trong injury dataset (trừ join keys) ──────────────
    injury_existing_cols = set(injury.columns) - {"PLAYER_ID", "SEASON"}
    # Chỉ giữ lại các cột MỚI từ stats + join keys
    new_cols = [c for c in stats_all.columns
                if c not in injury_existing_cols or c in ("PLAYER_ID","SEASON")]
    stats_clean = stats_all[new_cols]

    # ── Merge vào injury dataset ──────────────────────────────────────────────
    enriched = injury.merge(stats_clean, on=["PLAYER_ID","SEASON"], how="left")

    # ── Report ────────────────────────────────────────────────────────────────
    new_feature_count = len(enriched.columns) - len(injury.columns)
    print(f"\n=== Kết quả ===")
    print(f"  Injury dataset gốc : {len(injury.columns)} columns")
    print(f"  Features mới thêm  : {new_feature_count} columns")
    print(f"  Dataset sau merge  : {len(enriched.columns)} columns × {len(enriched)} rows")

    missing_pct = enriched.isnull().mean().sort_values(ascending=False)
    high_missing = missing_pct[missing_pct > 0.3]
    if not high_missing.empty:
        print(f"\n  Columns có >30% missing (cần review):")
        for col, pct in high_missing.items():
            print(f"    {col:<40} {pct*100:.1f}%")

    # ── Save ──────────────────────────────────────────────────────────────────
    enriched.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved → {OUTPUT_FILE}")

    # Print final column list
    print(f"\nTất cả {len(enriched.columns)} columns:")
    for i, col in enumerate(enriched.columns, 1):
        tag = " (new)" if col not in injury.columns else ""
        print(f"  {i:>3}. {col}{tag}")


if __name__ == "__main__":
    main()