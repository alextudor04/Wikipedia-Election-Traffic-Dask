"""
Election-day Wikipedia traffic: parallel filtering with Dask delayed

Read the Wikipedia traffic files in parallel with Dask (one delayed
task per file), keep rows whose page title contains "Barack_Obama",
and combine the results into one pandas data frame saved to disk.

Usage:
    python filter_day.py <data_dir> [output_csv]
    N_TEST_FILES=2 python filter_day.py <data_dir>   # small test first
"""

import csv
import glob
import os
import sys
import time

import dask
import pandas as pd

pd.set_option("display.max_columns", None)  # show all columns
pd.set_option("display.width", 200)         # don't wrap output

COLS = ["date", "time", "language", "page", "hits", "size"]


def read_and_filter(path, pattern="Barack_Obama"):
    """Read one file; keep rows whose page contains `pattern`."""
    df = pd.read_csv(
        path,
        sep=" ",
        header=None,
        names=COLS,                  # short lines -> NaN, not error
        dtype={"date": str, "time": str},  # keep leading zeros
        quoting=csv.QUOTE_NONE,      # quotes are literal text
        on_bad_lines="skip",         # skip lines with > 6 fields
    )
    keep = df["page"].str.contains(pattern, regex=False, na=False)
    return df.loc[keep]              # return only matching rows


def main():
    default_dir = f"/tmp/{os.environ.get('USER', 'user')}/wiki_day"
    data_dir = sys.argv[1] if len(sys.argv) > 1 else default_dir
    out_file = sys.argv[2] if len(sys.argv) > 2 else "obama_election_day.csv"

    # number of workers = cores Slurm gave us
    n_workers = int(os.getenv("SLURM_NTASKS",
                              os.getenv("SLURM_CPUS_PER_TASK", 4)))

    # part-* skips the empty _SUCCESS file
    files = sorted(glob.glob(os.path.join(data_dir, "part-*")))
    n_test = os.getenv("N_TEST_FILES")
    if n_test is not None:           # optional small test
        files = files[: int(n_test)]
    print(f"Processing {len(files)} files with {n_workers} workers",
          flush=True)

    # processes scheduler: pure-Python parsing, avoids the GIL
    dask.config.set(scheduler="processes", num_workers=n_workers)
    # lazy tasks, one per file (nothing runs yet)
    tasks = [dask.delayed(read_and_filter)(f) for f in files]

    t0 = time.time()
    results = dask.compute(tasks)[0]  # run all tasks in parallel
    print(f"Dask read + filter time: {time.time() - t0:.1f} sec",
          flush=True)

    obama = pd.concat(results, ignore_index=True)  # one data frame
    print(f"Rows kept: {len(obama)}")
    print(obama.head())

    obama.to_csv(out_file, index=False)
    print(f"Saved to {out_file}")


# workers import this file; only run main() when executed directly
if __name__ == "__main__":
    main()