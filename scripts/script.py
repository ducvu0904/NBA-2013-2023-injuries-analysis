import pandas as pd

def main() -> None:
    input_path = "../data/processed/Processed_dataset.csv"
    output_path = "../data/processed/Injury_event.csv"

    df = pd.read_csv(input_path)
    if "DAYS_MISSED" not in df.columns:
        raise KeyError("Column DAYS_MISSED not found in input dataset.")

    df["DAYS_MISSED"] = pd.to_numeric(df["DAYS_MISSED"], errors="coerce")
    injury_events = df[df["DAYS_MISSED"].fillna(0) > 0].copy()

    injury_events.to_csv(output_path, index=False)
    print(f"Saved {len(injury_events):,} rows to {output_path}")


if __name__ == "__main__":
    main()

