from __future__ import annotations

import subprocess
import sys
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parent

STAGES = [
    "init_clickhouse.py",
    "01_load_bronze.py",
    "02_transform_silver.py",
    "03_build_gold_and_load_clickhouse.py",
]


def main() -> None:
    for stage in STAGES:
        print(f"Running {stage}")
        subprocess.run([sys.executable, str(JOBS_DIR / stage)], check=True)


if __name__ == "__main__":
    main()
