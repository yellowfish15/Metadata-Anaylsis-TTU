'''
This program converts fits files into hdf5 files
and extracts metadata attribute information into
separated value format for future analysis

Created by: Yu Lim
'''
import os, h5py, numbers, statistics, sys
import numpy as np
import fnmatch, argparse, datetime
from mpi4py import MPI

# counts the total frequency of each type among all files in all directories
data = { # json object
    "total_statistics": {
        "sum_depth": 0, # sum of all depths among all files
        "files_processed": 0,
        "directories_processed": 0,
    }
} 

endl = '\t'

def analyze_file_recur(f, rel_path):
    global endl
    depth = 0
    for key in f[rel_path].keys(): # all content under this group
        item = f[rel_path][key]
        # process each attribute in this data_object
        for attr_key in item.attrs.keys():
            # attribute unique names
            print(attr_key, end=endl)

            vals = []
            if isinstance(item.attrs[attr_key], np.ndarray):
                for e in item.attrs[attr_key]:
                    vals.append(e)
            else:
                vals = [item.attrs[attr_key]]
            for i in range(len(vals)):
                val = vals[i]
                if isinstance(val, bytes):
                    vals[i] = val.decode('utf-8')
                elif type(val).__module__ == np.__name__:
                    vals[i] = val.item()
            val_type = type(vals[0])
            # value type
            if val_type == int:
                print('int', end=endl)
            elif val_type == float:
                print('float', end=endl)
            elif val_type == str:
                print('str', end=endl)
            else:
                print('other', end=endl)
            print(len(vals), end=endl)
            for i in range(len(vals)-1):
                print(vals[i],end='~~')
            print(vals[len(vals)-1])

        if isinstance(f[rel_path][key], h5py.Group): # if this object is a group
            depth = max(depth, analyze_file_recur(f, rel_path+"/"+key))
        # else: this object is a dataset
    return depth+1

file_count=0 # of files processed

# perform analysis on all ".fits" files in directory "curr_dir"
def analyze_dir(curr_dir, rank, size):
    global data, file_count
    os.chdir(os.path.join(os.getcwd(), curr_dir))

    '''
    Process all files in current directory (same as this file)
    ending with file extension ".h5"
    '''
    for file_ in os.listdir(os.getcwd()):
        if file_.endswith(".h5"):
            try:
                if file_count%size == rank:
                    f = h5py.File(file_, 'r') # open hdf5 file
                    file_depth = analyze_file_recur(f, '.')
                    data["total_statistics"]["sum_depth"] += file_depth
                    data["total_statistics"]["files_processed"]+=1
                file_count += 1
            except Exception as e:
                print(e)

# traverse through the current directory and subdirectories
def traverse_dir_recur(curr_dir_rel, rank, size):
    global data
    analyze_dir(curr_dir_rel, rank, size)
    path_abs = os.getcwd()
    directory_contents = os.listdir(path_abs)
    for item in directory_contents: # each item is a file/folder in the current directory
        if os.path.isdir(item): # if this item is a subdirectory
            traverse_dir_recur(item, rank, size) # recurse on the subdirectory
            os.chdir(path_abs) # switch back to current directory when done traversing
    data["total_statistics"]["directories_processed"] += 1

def process_console_args():
    parser = argparse.ArgumentParser('sdss_reader_sv_mpi.py')
    parser.add_argument('-d', '--dir')
    args = parser.parse_args()
    return args

def main():
    args = process_console_args()

    mpi_rank = MPI.COMM_WORLD.Get_rank()
    mpi_size = MPI.COMM_WORLD.Get_size()

    sys.stdout = open('output_raw'+str(mpi_rank)+'.txt', 'w') # Direct standard output to file
    traverse_dir_recur(args.dir, mpi_rank, mpi_size)
    print('\tEND\t')
    print(data)

if __name__ == "__main__":
    main()
