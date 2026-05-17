"""
Bar chart of FloodWaitError durations per day - Worker 0, Nov 6-11 2023.
Each day's errors are distributed evenly across its x-section so the
density of bars reflects how frequently Telegram imposed rate-limiting.

Reads from a JSON file produced by extract_fwe.py when one is provided;
otherwise uses the embedded data below.

Usage:
    python plot_fwe_days.py                  # embedded data
    python plot_fwe_days.py fwe_data.json    # from extractor
"""

import json
import argparse
import numpy as np
import matplotlib.pyplot as plt

_EMBEDDED_DATA = {
    '2023-11-06': [639, 556, 439, 564, 555, 644, 675, 700],
    '2023-11-07': [562, 535, 742, 655, 632, 543, 686, 654, 614, 399, 693, 514, 660, 549, 320, 523, 699, 404, 562, 637, 516, 657, 583, 523, 441],
    '2023-11-08': [576, 337, 625, 200, 158, 587, 327, 365, 629, 665, 611, 636, 652, 556, 525, 531, 530, 671, 408, 551, 236, 481, 502, 554, 648, 452, 212, 189, 460, 548, 564, 498, 492, 603, 480, 558, 143],
    '2023-11-09': [123, 478, 312, 572, 357, 226, 436, 544, 629, 651, 535, 561, 151, 234, 363, 635, 268, 128, 388, 539, 402, 509, 467, 385, 374, 459, 293, 519, 290, 395, 222, 652, 472, 566],
    '2023-11-10': [499, 616, 605, 566, 333, 529, 580, 471, 440, 448, 599, 181, 472, 477, 171, 78, 146, 283, 240, 167, 575, 397, 653, 377, 372, 559, 374, 591, 182, 685],
    '2023-11-11': [496, 488, 72, 397, 610, 279, 520, 191, 428, 405, 491, 137, 317, 401, 616, 302, 176, 627, 321, 285, 342, 192, 412, 352, 177, 114, 645, 554],
}

def _load_data(path: str | None) -> dict:
    if path:
        with open(path) as f:
            return json.load(f)
    return _EMBEDDED_DATA


def plot(data: dict) -> None:
    days = list(data.keys())
    bar_width = 0.01

    plt.figure(figsize=(12, 6))
    for i, (day, day_values) in enumerate(data.items()):
        x_positions = np.linspace(i, i + 1, len(day_values))
        plt.bar(x_positions, day_values, width=bar_width, align="edge", label=day)

    plt.title("Worker 0 - FloodWaitError durations (Nov 6-11 2023)", fontsize=20)
    plt.xlabel("Days", fontsize=20)
    plt.ylabel("Wait duration (seconds)", fontsize=20)
    plt.xticks(np.arange(len(days)) + 0.5, days, fontsize=18)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot FWE durations per day")
    parser.add_argument("data_file", nargs="?", default=None,
                        help="JSON produced by extract_fwe.py (omit to use embedded data)")
    args = parser.parse_args()
    plot(_load_data(args.data_file))
