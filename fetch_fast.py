"""
fill_team_fast.py
=================
Điền cột TEAM bằng LeagueDashPlayerStats.
Chỉ cần 10 lần gọi API (1 per season) thay vì 4358 lần.

Với player bị trade giữa mùa, NBA API trả về team họ chơi
nhiều games nhất trong season đó (đây là hành vi mặc định
của LeagueDashPlayerStats).

Cài đặt:
    pip install nba_api pandas

Chạy:
    python fill_team_fast.py

Output:
    injury_stat_filled.csv
    season_roster_cache.csv   -- cache 10 seasons, dùng lại được
"""

import time
import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats

INPUT_FILE   = "injury+stat.csv"
OUTPUT_FILE  = "injury_stat_filled.csv"
CACHE_FILE   = "season_roster_cache.csv"

REQUEST_DELAY = 1.5  # giây, đủ an toàn với rate limit NBA API

SEASONS = [
    "2013-14", "2014-15", "2015-16", "2016-17", "2017-18",
    "2018-19", "2019-20", "2020-21", "2021-22", "2022-23",
]

# NBA API trả về abbreviation, map sang tên trong dataset gốc
ABBR_TO_NAME = {
    "ATL": "Hawks",       "BOS": "Celtics",     "BKN": "Nets",
    "NJN": "Nets",        "CHA": "Hornets",     "CHO": "Hornets",
    "CHI": "Bulls",       "CLE": "Cavaliers",   "DAL": "Mavericks",
    "DEN": "Nuggets",     "DET": "Pistons",     "GSW": "Warriors",
    "HOU": "Rockets",     "IND": "Pacers",      "LAC": "Clippers",
    "LAL": "Lakers",      "MEM": "Grizzlies",   "MIA": "Heat",
    "MIL": "Bucks",       "MIN": "Timberwolves","NOH": "Pelicans",
    "NOP": "Pelicans",    "NYK": "Knicks",      "OKC": "Thunder",
    "ORL": "Magic",       "PHI": "76ers",       "PHX": "Suns",
    "POR": "Blazers",     "SAC": "Kings",       "SAS": "Spurs",
    "TOR": "Raptors",     "UTA": "Jazz",        "WAS": "Wizards",
    "NOK": "Pelicans",    "SEA": "Thunder",     "VAN": "Grizzlies",
    "TOT": None,          # "TOT" = tổng hợp nhiều team, bỏ qua
}

def season_api_to_short(s: str) -> str:
    """'2022-23' → '22-23'"""
    return s[2:]


def fetch_season_roster(season: str) -> pd.DataFrame:
    """
    Gọi LeagueDashPlayerStats cho 1 season.
    Trả về DataFrame với cột: PLAYER_ID, TEAM_ABBREVIATION
    """
    print(f"  Fetching {season} ...", end=" ", flush=True)
    stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star="Regular Season",
        per_mode_detailed="PerGame",
        timeout=30,
    )
    df = stats.get_data_frames()[0][["PLAYER_ID", "TEAM_ABBREVIATION"]].copy()
    df["SEASON"] = season_api_to_short(season)
    df["TEAM_NAME"] = df["TEAM_ABBREVIATION"].map(ABBR_TO_NAME)
    print(f"→ {len(df)} players")
    return df


def build_roster_map() -> pd.DataFrame:
    """
    Load từ cache nếu có, gọi API cho các season còn thiếu.
    Trả về DataFrame: PLAYER_ID, SEASON, TEAM_NAME
    """
    # Load cache
    try:
        cached = pd.read_csv(CACHE_FILE)
        cached_seasons = set(
            cached["SEASON"].map(lambda s: f"20{s[:2]}-{s[3:]}" if int(s[:2]) >= 13 else f"20{s[:2]}-{s[3:]}")
        )
        # Đơn giản hơn: so sánh trực tiếp short format
        cached_short = set(cached["SEASON"].unique())
        print(f"Cache loaded: {len(cached)} rows, seasons: {sorted(cached_short)}")
    except FileNotFoundError:
        cached = pd.DataFrame()
        cached_short = set()
        print("No cache found, fetching all seasons")

    # Fetch các season chưa có trong cache
    new_frames = []
    for season_api in SEASONS:
        short = season_api_to_short(season_api)
        if short in cached_short:
            print(f"  {season_api} → from cache")
            continue
        frame = fetch_season_roster(season_api)
        new_frames.append(frame)
        time.sleep(REQUEST_DELAY)

    # Gộp cache + mới
    all_frames = [cached] + new_frames if not cached.empty else new_frames
    roster_map = pd.concat(all_frames, ignore_index=True)

    # Save cache
    roster_map.to_csv(CACHE_FILE, index=False)
    print(f"\nCache saved → {CACHE_FILE}")
    return roster_map


def main():
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} rows | TEAM missing: {df['TEAM'].isna().sum()}\n")

    # Bước 1: Build roster map (10 API calls)
    print("=== Fetching roster data (10 API calls) ===")
    roster = build_roster_map()

    # Bỏ rows TOT (player chơi cho nhiều team, NBA API không assign 1 team cụ thể)
    roster = roster[roster["TEAM_NAME"].notna()].copy()

    # Bước 2: Tạo lookup dict: (player_id, season_short) -> team_name
    lookup = dict(zip(
        zip(roster["PLAYER_ID"], roster["SEASON"]),
        roster["TEAM_NAME"]
    ))
    print(f"\nLookup map built: {len(lookup)} entries")

    # Bước 3: Điền TEAM
    def fill_team(row):
        if pd.notna(row["TEAM"]):
            return row["TEAM"]
        return lookup.get((row["PLAYER_ID"], row["SEASON"]))

    df["TEAM"] = df.apply(fill_team, axis=1)

    # Bước 4: Report
    filled_count   = df["TEAM"].notna().sum()
    missing_count  = df["TEAM"].isna().sum()
    print(f"\n=== Kết quả ===")
    print(f"  Tổng rows      : {len(df)}")
    print(f"  TEAM có dữ liệu: {filled_count} ({filled_count/len(df)*100:.1f}%)")
    print(f"  Vẫn còn thiếu : {missing_count}")

    if missing_count > 0:
        still_missing = df[df["TEAM"].isna()][["PLAYER_ID","PLAYER_NAME","SEASON"]].drop_duplicates()
        print(f"\n  Players vẫn thiếu (có thể là two-way contract / G-League):")
        print(still_missing.to_string(index=False))

    # Bước 5: Save
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()