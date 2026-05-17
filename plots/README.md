# plots - Crawler Analytics Scripts

Analysis and visualisation scripts for the Telegram crawler runs (Nov 2023).

## Scripts

| Script | Input | Output | What it shows / does |
|---|---|---|---|
| `extract_link_evaluations.py` | crawler log | `link_evaluations.json` | Parses "Data collected" events - counts per day/hour |
| `extract_fwe.py` | crawler log | `fwe_data.json` | Parses FloodWaitError durations per day |
| `extract_join_throughput.py` | crawler log | `join_throughput.json` | Parses successful join events - counts per day/hour |
| `plot_link_evaluations.py` | `link_evaluations.json` | plot | Hourly link-evaluation throughput - 5-worker run (Oct 31-Nov 3) |
| `plot_join_throughput.py` | `join_throughput.json` or embedded | plot | Hourly join throughput - 2-worker run (Nov 6-23) |
| `plot_fwe_cdf.py` | `fwe_data.json` or embedded | plot | Empirical CDF of FloodWaitError durations (~1 000 events) |
| `plot_fwe_days.py` | `fwe_data.json` or embedded | plot | Bar chart of FWE durations per day - Worker 0, Nov 6-11 |

## Pipeline

```
crawler stdout log
        │
        ├── extract_link_evaluations.py  →  link_evaluations.json  →  plot_link_evaluations.py
        ├── extract_join_throughput.py   →  join_throughput.json   →  plot_join_throughput.py
        └── extract_fwe.py              →  fwe_data.json          →  plot_fwe_cdf.py
                                                                      plot_fwe_days.py
```

All plot scripts also work standalone using embedded data (no log file needed).

## Usage

```bash
pip install numpy matplotlib

# Full pipeline - reproduce everything from the raw log
python extract_link_evaluations.py ordered_result_6_11_23.txt
python extract_join_throughput.py  ordered_result_6_11_23.txt
python extract_fwe.py              ordered_result_6_11_23.txt

python plot_link_evaluations.py link_evaluations.json
python plot_join_throughput.py  join_throughput.json
python plot_fwe_cdf.py          fwe_data.json
python plot_fwe_days.py         fwe_data.json

# Standalone - embedded data, no log file required
python plot_join_throughput.py
python plot_fwe_cdf.py
python plot_fwe_days.py
```

## Key findings

**Join throughput** (`plot_join_throughput.py`)  
The 2-worker run sustained 40-55 joins/hour consistently across 18 days after
the first few hours of warmup. Throughput dips in the evenings are visible and
consistent, likely caused by Telegram's time-of-day rate limits.

**FloodWaitError distribution** (`plot_fwe_cdf.py`)  
~95 % of enforced wait times fall between 60 and 720 seconds. Three extreme
outliers (9 453 s, 17 640 s, 29 368 s) expose Telegram's adaptive policy:
sustained high-frequency requests trigger multi-hour bans rather than the
standard 5-12-minute penalties.

**FWE trend over days** (`plot_fwe_days.py`)  
Nov 6-7 show high and variable penalties (500-740 s). By Nov 8 onward the
distribution narrows to ~300 s, suggesting Telegram's rate-limiter stabilises
once request patterns become regular.
