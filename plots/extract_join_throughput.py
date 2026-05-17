"""
Parse a crawler log file and count successful group joins per day and hour.
Writes results to a JSON file consumed by plot_join_throughput.py.

Usage:
    python extract_join_throughput.py ordered_result_6_11_23.txt
    python extract_join_throughput.py ordered_result_6_11_23.txt -o join_throughput.json
"""

import re
import json
import argparse
from datetime import datetime

# Matches both private-hash and public join success log lines:
#   2023-11-07 15:23:45.123456 - [WORKER n.0] [+] [HASH] Joined: GroupName (hash)
#   2023-11-07 15:23:45.123456 - [WORKER n.0] [+] [PUBLIC] Joined: GroupName (link)
_JOIN_RE = re.compile(
    r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*\[\+\].*Joined:'
)


def extract_join_throughput(input_file: str, output_file: str) -> None:
    timestamps = []
    with open(input_file) as f:
        for line in f:
            m = _JOIN_RE.search(line)
            if m:
                timestamps.append(
                    datetime.strptime(m.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
                )

    if not timestamps:
        raise RuntimeError(f"No join-success events found in {input_file!r}")

    dates = sorted({t.date() for t in timestamps})
    hourly: dict[str, list[int]] = {str(d): [0] * 24 for d in dates}
    for t in timestamps:
        hourly[str(t.date())][t.hour] += 1

    with open(output_file, "w") as f:
        json.dump(hourly, f, indent=2)

    print(
        f"Extracted {len(timestamps)} join events across {len(dates)} days → {output_file}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract hourly group-join counts from crawler log"
    )
    parser.add_argument("input", help="Crawler log file (e.g. ordered_result_6_11_23.txt)")
    parser.add_argument("-o", "--output", default="join_throughput.json",
                        help="Output JSON file (default: join_throughput.json)")
    args = parser.parse_args()
    extract_join_throughput(args.input, args.output)
