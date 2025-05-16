#!/bin/bash

#PBS -P ng72 
#PBS -q normal
#PBS -l walltime=24:00:00,mem=190GB 
#PBS -l ncpus=48
#PBS -l jobfs=32gb
#PBS -l storage=gdata/nf33+gdata/dk92+gdata/qx55+gdata/hh5+scratch/nf33
 
#Set up conda/shell environments 
module use /g/data/dk92/apps/Modules/modulefiles
module use /g/data/hh5/public/modules

module load gadi_jupyterlab/23.02
module load conda/analysis3
source /scratch/nf33/public/hackathon_env/bin/activate

jupyter.ini.sh -D

python /home/548/ab4502/working/hk25-AusNode-coastal/analysis/calculate_coastline_angles.py