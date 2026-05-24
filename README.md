# NBA 2013-2023 Injuries Analysis

## Project structure

- data/raw: source datasets (original inputs)
- data/interim: intermediate outputs from collection/enrichment
- data/processed: cleaned and analysis-ready datasets
- data/cache: cached API pulls (nba_stats, season_roster_cache.csv)
- notebooks/eda: exploratory notebooks
- notebooks/analysis: analysis and visualization notebooks
- notebooks/etl: data preparation notebooks
- scripts/collect: data collection/enrichment scripts
- output: generated figures and exports

## Data flow (typical)

1. scripts/collect/fetch_fast.py
   - input: data/raw/injury+stat.csv
   - output: data/interim/injury_stat_filled.csv
   - cache: data/cache/season_roster_cache.csv

2. scripts/collect/fetch_advanced.py
   - input: data/interim/injury_stat_filled.csv
   - output: data/interim/injury_stat_enriched.csv
   - cache: data/cache/nba_stats/

3. notebooks/etl/data_cleaning.ipynb
   - input: data/interim/injury_stat_enriched.csv
   - output: data/processed/Processed_dataset.csv

4. notebooks/etl/Split_data.ipynb
   - input: data/processed/Processed_dataset.csv
   - output: data/processed/ankle_injuries.csv, data/processed/knee_injuries.csv

5. notebooks/eda/EDA.ipynb
   - input: data/processed/Processed_dataset.csv
   - output: output/ (plots)

6. notebooks/analysis/Deep_dive_analysis.ipynb
   - input: data/processed/ankle_injuries.csv, data/processed/knee_injuries.csv
   - output: data/processed/spearman_correlation_values_light.csv

## Notes

- If you move files again, update the relative paths in notebooks and scripts.
- Re-running notebooks refreshes saved outputs with the new paths.
