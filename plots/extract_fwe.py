"""
Parse a crawler log file and extract Flood Wait Error durations per day.
Writes results to a JSON file consumed by plot_fwe_cdf.py and plot_fwe_days.py.

Usage:
    python extract_fwe.py ordered_result_6_11_23.txt
    python extract_fwe.py ordered_result_6_11_23.txt -o fwe_data.json
"""

import re
import json
import argparse
from datetime import datetime
from collections import defaultdict

_FWE_RE = re.compile(
    r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'
    r'.*?Flood Error: Waiting for (?P<duration>\d+) seconds'
)


def extract_fwe(input_file: str, output_file: str) -> None:
    durations_by_day: dict[str, list[int]] = defaultdict(list)
    with open(input_file) as f:
        for line in f:
            m = _FWE_RE.search(line)
            if m:
                ts = datetime.strptime(m.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
                durations_by_day[str(ts.date())].append(int(m.group("duration")))

    if not durations_by_day:
        raise RuntimeError(f"No FloodWaitError events found in {input_file!r}")

    total = sum(len(v) for v in durations_by_day.values())
    result = dict(sorted(durations_by_day.items()))
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Extracted {total} FWE events across {len(durations_by_day)} days → {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Flood Wait Error durations from crawler log"
    )
    parser.add_argument("input", help="Crawler log file (e.g. ordered_result_6_11_23.txt)")
    parser.add_argument("-o", "--output", default="fwe_data.json",
                        help="Output JSON file (default: fwe_data.json)")
    args = parser.parse_args()
    extract_fwe(args.input, args.output)
