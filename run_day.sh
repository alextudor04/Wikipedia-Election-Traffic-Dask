#!/bin/bash
# Slurm settings: 4 cores on 1 node, 30 min
#SBATCH --job-name=wiki_day
#SBATCH --partition=low
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=00:30:00
#SBATCH --output=run_day.out
#SBATCH --error=run_day.err

# one-day dataset; set before submitting: DAY_DATA=/path/to/data sbatch run_day.sh
DATA_SRC=${DAY_DATA:?set DAY_DATA to the one-day data directory}
TMP_DATA=/tmp/$USER/wiki_day
# delete the copy on exit, even if Python fails
trap 'rm -rf "$TMP_DATA"' EXIT

# copy to /tmp on whichever node Slurm picks
mkdir -p "$TMP_DATA"
cp "$DATA_SRC"/* "$TMP_DATA"/
echo "Running on $(hostname); copied $(ls "$TMP_DATA" | wc -l) files"

python filter_day.py "$TMP_DATA" obama_election_day.csv
# clean up /tmp (shared, only cleared on reboot)
rm -rf "$TMP_DATA"