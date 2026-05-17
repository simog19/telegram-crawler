"""
Plot hourly join throughput for the 2-worker crawler run (Nov 6-23 2023).

Reads from a JSON file produced by extract_join_throughput.py when one is
provided; otherwise uses the embedded data extracted from the crawler log.

Usage:
    python plot_join_throughput.py                         # embedded data
    python plot_join_throughput.py join_throughput.json    # from extractor
"""

import json
import argparse
import numpy as np
import matplotlib.pyplot as plt

_EMBEDDED_DATA = {
    '2023-11-06': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 43, 42, 40, 40],
    '2023-11-07': [42, 41, 40, 41, 40, 45, 41, 46, 44, 48, 45, 37, 20, 19, 21, 20, 20, 9, 4, 15, 20, 19, 38, 42],
    '2023-11-08': [39, 43, 48, 55, 48, 43, 40, 39, 39, 41, 42, 41, 48, 39, 38, 41, 46, 29, 24, 28, 14, 22, 39, 46],
    '2023-11-09': [47, 44, 45, 39, 40, 40, 39, 38, 35, 44, 38, 40, 38, 40, 42, 41, 39, 32, 22, 18, 18, 18, 21, 38],
    '2023-11-10': [41, 37, 42, 42, 40, 39, 39, 40, 46, 45, 45, 50, 44, 38, 40, 40, 28, 13, 7, 16, 17, 23, 25, 37],
    '2023-11-11': [42, 49, 53, 47, 40, 39, 39, 40, 41, 41, 39, 43, 42, 44, 42, 39, 41, 46, 41, 47, 53, 36, 28, 47],
    '2023-11-12': [59, 47, 56, 55, 58, 56, 60, 49, 50, 45, 48, 40, 48, 44, 47, 49, 43, 46, 38, 48, 53, 37, 26, 42],
    '2023-11-13': [57, 46, 50, 44, 44, 49, 46, 47, 46, 49, 46, 48, 43, 40, 49, 43, 43, 41, 42, 45, 52, 34, 19, 33],
    '2023-11-14': [50, 42, 45, 45, 44, 42, 43, 43, 54, 52, 54, 52, 52, 44, 49, 39, 39, 52, 40, 49, 49, 38, 29, 44],
    '2023-11-15': [47, 42, 46, 43, 43, 48, 51, 46, 43, 48, 50, 47, 43, 39, 43, 39, 36, 49, 38, 48, 49, 31, 20, 34],
    '2023-11-16': [44, 39, 44, 44, 47, 46, 49, 41, 52, 50, 45, 43, 47, 46, 48, 47, 47, 50, 39, 48, 51, 39, 26, 46],
    '2023-11-17': [51, 50, 49, 51, 52, 50, 52, 52, 50, 52, 52, 50, 54, 44, 46, 51, 42, 53, 36, 45, 51, 37, 20, 35],
    '2023-11-18': [52, 46, 52, 49, 47, 53, 51, 50, 51, 50, 54, 47, 47, 37, 50, 48, 46, 51, 39, 44, 50, 43, 27, 39],
    '2023-11-19': [50, 49, 46, 51, 45, 53, 55, 51, 47, 51, 52, 48, 47, 48, 48, 47, 38, 45, 38, 46, 45, 39, 25, 42],
    '2023-11-20': [49, 52, 44, 50, 56, 49, 54, 48, 54, 50, 53, 52, 49, 46, 49, 48, 46, 55, 41, 45, 51, 40, 26, 33],
    '2023-11-21': [49, 49, 49, 48, 55, 56, 53, 48, 52, 55, 46, 50, 45, 38, 48, 42, 41, 48, 36, 45, 54, 45, 27, 32],
    '2023-11-22': [46, 51, 45, 42, 53, 48, 51, 43, 48, 47, 52, 53, 50, 44, 42, 48, 43, 52, 42, 45, 53, 52, 29, 39],
    '2023-11-23': [48, 46, 43, 43, 49, 51, 45, 49, 46, 51, 51, 56, 50, 40, 48, 45, 36, 47, 42, 8, 0, 0, 0, 0],
}

def _load_data(path: str | None) -> dict:
    if path:
        with open(path) as f:
            return json.load(f)
    return _EMBEDDED_DATA


def plot(data: dict) -> None:
    dates = list(data.keys())

    fig, ax = plt.subplots(figsize=(20, 8))
    for i, date in enumerate(dates):
        x_vals = np.arange(24) + i * 24
        ax.plot(x_vals, data[date], ".-")

    ax.set_xticks(np.arange(len(dates)) * 24 + 12)
    ax.set_xticklabels(dates, rotation=30, ha="right", fontsize=16)
    ax.set_ylabel("Groups/channels joined per hour", fontsize=20)
    ax.set_title("2-worker crawler - join throughput (Nov 6-23 2023)", fontsize=20)
    plt.yticks(fontsize=18)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot hourly join throughput")
    parser.add_argument("data_file", nargs="?", default=None,
                        help="JSON produced by extract_join_throughput.py (omit to use embedded data)")
    args = parser.parse_args()
    plot(_load_data(args.data_file))
