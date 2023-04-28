#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 8 # number of cores
#SBATCH --mem 60GB # memory pool for all cores
#SBATCH -t 1-00:00 # time (D-HH:MM)


module load Anaconda3/2022.05
source activate dorieh
python generate_counts.py