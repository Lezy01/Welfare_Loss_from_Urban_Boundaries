#!/bin/bash

#SBATCH --job-name=final_project_city_edge
#SBATCH --output=logs/out_%A_%a.out
#SBATCH --error=logs/err_%A_%a.err
#SBATCH --account=macs30123
#SBATCH --partition=caslake
#SBATCH --array=0-328
#SBATCH --time=01:00:00
#SBATCH --mem=4G

module load python/anaconda-2022.05
source /software/python-anaconda-2022.05-el8-x86_64/etc/profile.d/conda.sh
conda activate final_project


TASK_FILE=/home/xinyu01/Final_project/Code/tasks.txt
LINE=$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" $TASK_FILE)
PROV=$(echo $LINE | cut -d',' -f1)
CITY=$(echo $LINE | cut -d',' -f2)

python rent_curve_mw.py --prov "$PROV" --city "$CITY"
