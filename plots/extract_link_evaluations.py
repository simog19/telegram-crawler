"""
Parse a crawler log file and count "Data collected" events per day and hour.
Writes results to a JSON file consumed by plot_link_evaluations.py.
"""

import re
import json
import argparse
from datetime import datetime

_TIMESTAMP_RE = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+')


def extract_link_evaluations(input_file: str, output_file: str) -> None:
    timestamps = []
    with open(input_file) as f:
        for line in f:
            if 'Data collected' in line:
                m = _TIMESTAMP_RE.search(line)
                if m:
                    timestamps.append(datetime.strptime(m.group(), "%Y-%m-%d %H:%M:%S.%f"))

    if not timestamps:
        raise RuntimeError(f"No 'Data collected' events found in {input_file!r}")

    dates = sorted({t.date() for t in timestamps})
    hourly: dict[str, list[int]] = {str(d): [0] * 24 for d in dates}
    for t in timestamps:
        hourly[str(t.date())][t.hour] += 1

    with open(output_file, "w") as f:
        json.dump(hourly, f, indent=2)

    print(f"Extracted {len(timestamps)} events across {len(dates)} days → {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract link evaluation counts from crawler log")
    parser.add_argument("input", help="Crawler log file (e.g. ordered_result_6_11_23.txt)")
    parser.add_argument("output", nargs="?", default="link_evaluations.json",
                        help="Output JSON file (default: link_evaluations.json)")
    args = parser.parse_args()
    extract_link_evaluations(args.input, args.output)
