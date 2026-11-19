#!/bin/bash
# Slurm settings: 16 cores on 1 node, 90 min
#SBATCH --job-name=wiki_month
#SBATCH --partition=low
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --time=01:30:00
#SBATCH --output=run_month.out
#SBATCH --error=run_month.err

# month dataset, read in place from each node's local disk
# set before submitting: MONTH_DATA=/path/to/data sbatch run_month.sh
MONTH_DATA=${MONTH_DATA:?set MONTH_DATA to the month data directory}
echo "Running on $(hostname) with $SLURM_NTASKS tasks"
python count_month.py "$MONTH_DATA" obama_month_counts.csv