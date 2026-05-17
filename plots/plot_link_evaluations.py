"""
Plot hourly link-evaluation throughput from the JSON produced by extract_link_evaluations.py.
"""

import json
import argparse
import numpy as np
import matplotlib.pyplot as plt


def plot_link_evaluations(data_file: str) -> None:
    with open(data_file) as f:
        data = json.load(f)

    dates = list(data.keys())

    fig, ax = plt.subplots(figsize=(20, 8))
    for i, date in enumerate(dates):
        x_vals = np.arange(24) + i * 24
        ax.plot(x_vals, data[date], ".-")

    ax.set_xticks(np.arange(len(dates)) * 24 + 12)
    ax.set_xticklabels(dates, rotation=30, ha="right", fontsize=16)
    ax.set_ylabel("Total number of evaluated links", fontsize=20)
    ax.set_title("5 workers - evaluated links per day and hour", fontsize=20)
    plt.yticks(fontsize=18)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot evaluated-link throughput")
    parser.add_argument("data_file", nargs="?", default="link_evaluations.json",
                        help="JSON produced by extract_link_evaluations.py")
    args = parser.parse_args()
    plot_link_evaluations(args.data_file)
