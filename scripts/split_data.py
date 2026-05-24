import pandas as pd

# Read source data
full_df = pd.read_csv("../data/processed/Processed_dataset.csv")

# Normalize text once to handle different spellings/cases (sprained ankle vs sprained_ankle)
injury_norm = full_df["INJURED_TYPE"].astype("string").str.strip().str.lower()
ankle_mask = injury_norm.str.contains(r"sprained[_\s]+ankle", na=False)

# Player-season groups that had sprained ankle
ankle_groups = full_df.loc[ankle_mask, ["PLAYER_ID", "SEASON"]]
ankle_groups = ankle_groups.drop_duplicates()

# Rows from those groups: keep only sprained ankle rows + healthy rows (NaN)
ankle_rows = full_df.merge(ankle_groups, on=["PLAYER_ID", "SEASON"], how="inner")
ankle_rows_norm = ankle_rows["INJURED_TYPE"].astype("string").str.strip().str.lower()
ankle_rows = ankle_rows[
    ankle_rows_norm.str.contains(r"sprained[_\s]+ankle", na=False)
    | ankle_rows["INJURED_TYPE"].isna()
]

# Player-season groups with no injury at all
no_injury_groups = (
    full_df.groupby(["PLAYER_ID", "SEASON"])["INJURED_TYPE"]
    .apply(lambda x: x.isna().all())
    .reset_index(name="NO_INJURY")
)
no_injury_groups = no_injury_groups[no_injury_groups["NO_INJURY"]][["PLAYER_ID", "SEASON"]]

no_injury_rows = full_df.merge(no_injury_groups, on=["PLAYER_ID", "SEASON"], how="inner")

# Final ankle dataset: only sprained ankle cohort + no-injury cohort
final_ankle_df = pd.concat([ankle_rows, no_injury_rows], ignore_index=True)
final_ankle_df = final_ankle_df.sample(frac=1, random_state=42).reset_index(drop=True)

final_ankle_df.to_csv("../data/processed/ankle_injuries.csv", index=False)
print("../data/processed/ankle_injuries.csv:", final_ankle_df.shape)


# Read source data
full_df = pd.read_csv("../data/processed/Processed_dataset.csv")

# Normalize text once to handle different spellings/cases (knee injury vs knee_injury)
injury_norm = full_df["INJURED_TYPE"].astype("string").str.strip().str.lower()
knee_mask = injury_norm.str.contains(r"knee[_\s]+injury", na=False)

# Player-season groups that had knee injury
knee_groups = full_df.loc[knee_mask, ["PLAYER_ID", "SEASON"]]
knee_groups = knee_groups.drop_duplicates()

# Rows from those groups: keep only knee injury rows + healthy rows (NaN)
knee_rows = full_df.merge(knee_groups, on=["PLAYER_ID", "SEASON"], how="inner")
knee_rows_norm = knee_rows["INJURED_TYPE"].astype("string").str.strip().str.lower()
knee_rows = knee_rows[
    knee_rows_norm.str.contains(r"knee[_\s]+injury", na=False)
    | knee_rows["INJURED_TYPE"].isna()
]

# Player-season groups with no injury at all
no_injury_groups = (
    full_df.groupby(["PLAYER_ID", "SEASON"])["INJURED_TYPE"]
    .apply(lambda x: x.isna().all())
    .reset_index(name="NO_INJURY")
)
no_injury_groups = no_injury_groups[no_injury_groups["NO_INJURY"]][["PLAYER_ID", "SEASON"]]

no_injury_rows = full_df.merge(no_injury_groups, on=["PLAYER_ID", "SEASON"], how="inner")

# Final knee dataset: only knee injury cohort + no-injury cohort
final_knee_df = pd.concat([knee_rows, no_injury_rows], ignore_index=True)
final_knee_df = final_knee_df.sample(frac=1, random_state=42).reset_index(drop=True)

final_knee_df.to_csv("../data/processed/knee_injuries.csv", index=False)
print("../data/processed/knee_injuries.csv:", final_knee_df.shape)

def main() -> None:
    input_path = "data/processed/Processed_dataset.csv"
    output_path = "data/processed/Injury_event.csv"

    df = pd.read_csv(input_path)
    if "DAYS_MISSED" not in df.columns:
        raise KeyError("Column DAYS_MISSED not found in input dataset.")

    df["DAYS_MISSED"] = pd.to_numeric(df["DAYS_MISSED"], errors="coerce")
    injury_events = df[df["DAYS_MISSED"].fillna(0) > 0].copy()

    injury_events.to_csv(output_path, index=False)
    print(f"Saved {len(injury_events):,} rows to {output_path}")


if __name__ == "__main__":
    main()