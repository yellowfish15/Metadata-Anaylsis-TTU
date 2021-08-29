#!/bin/bash
#SBATCH --job-name=Read_HDF5_Files_To_JSON_Python
#SBATCH --output=%x.o%j
#SBATCH --error=%x.e%j
#SBATCH --partition nocona
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=4
#SBATCH --time=48:00:00
##SBATCH --mem-per-cpu=3994MB  ##3.9GB, Modify based on needs   

. $HOME/conda/etc/profile.d/conda.sh
conda activate

conda install mpi4py
conda install numpy
conda install h5py

python /home/lim11950/sdss_reader_json.py -d /home/lim11950/spectro/redux/103