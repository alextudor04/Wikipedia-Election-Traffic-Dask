"""
Month-scale Wikipedia traffic: Dask bag pipeline

Use a Dask bag (a distributed data structure) to read the
Wikipedia files and keep English pages whose title contains
"Barack_Obama". Convert the bag to a Dask data frame, sum hits by
day-hour, and save the counts to a CSV.

Usage:
    python count_month.py <data_dir> [output_csv]
    N_TEST_FILES=3 python count_month.py <data_dir> test_month.csv
"""

import glob
import os
import sys
import time

import dask
import dask.bag as db
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)


def parse_obama_en(line):
    """Return (date, time, hits) for an English Obama line, else None."""
    vals = line.split(" ")
    # need 6 fields, English, Obama in page title
    if (len(vals) != 6 or vals[2] != "en"
            or "Barack_Obama" not in vals[3]):
        return None
    try:
        hits = int(vals[4])
    except ValueError:               # hits field not a number
        return None
    return (vals[0], vals[1], hits)


def main():
    data_dir = sys.argv[1]
    out_file = (sys.argv[2] if len(sys.argv) > 2
                else "obama_month_counts.csv")

    # number of workers = cores Slurm gave us
    n_workers = int(os.getenv("SLURM_NTASKS",
                              os.getenv("SLURM_CPUS_PER_TASK", 4)))
    # processes scheduler: pure-Python parsing, avoids the GIL
    dask.config.set(scheduler="processes", num_workers=n_workers)

    # part-* skips the empty _SUCCESS file
    files = sorted(glob.glob(os.path.join(data_dir, "part-*")))
    n_test = os.getenv("N_TEST_FILES")
    if n_test is not None:           # optional small test
        files = files[: int(n_test)]
    print(f"Processing {len(files)} files with {n_workers} workers",
          flush=True)

    # bag of lines; each file is a partition
    wiki = db.read_text(files)

    obama = (
        wiki.filter(lambda line: "Barack_Obama" in line)  # cheap first
        .map(parse_obama_en)                  # line -> tuple or None
        .filter(lambda rec: rec is not None)  # drop non-matches
    )

    # bag -> Dask data frame, then sum hits by day-hour (still lazy)
    ddf = obama.to_dataframe(
        meta={"date": str, "time": str, "hits": int})
    counts_lazy = ddf.groupby(["date", "time"])["hits"].sum()

    t0 = time.time()
    counts = counts_lazy.compute()   # one pass: read, filter, group
    print(f"Dask read + filter + groupby time: "
          f"{time.time() - t0:.1f} sec", flush=True)

    counts = (counts.reset_index()
              .sort_values(["date", "time"])
              .reset_index(drop=True))
    print(f"Number of day-hours: {len(counts)}")
    print(counts.head(10))

    counts.to_csv(out_file, index=False)
    print(f"Saved to {out_file}")


# workers import this file; only run main() when executed directly
if __name__ == "__main__":
    main()