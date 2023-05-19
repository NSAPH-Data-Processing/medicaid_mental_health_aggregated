#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 60GB # memory pool for all cores
#SBATCH -t 0-05:00 # time (D-HH:MM)
#SBATCH --array=1999-2012

module load Anaconda3/2022.05
source activate medicare_QC
python generate_counts.py --year $SLURM_ARRAY_TASK_ID
