'''
This program converts fits files into hdf5 files
and extracts metadata attribute information into
json format for future analysis

Created by: Yu Lim
'''

import os
import h5py
import json
import numbers
import statistics
import numpy as np
import fnmatch, sys
import argparse
import datetime
from mpi4py import MPI


# counts the total frequency of each type among all files in all directories
data = { # json object
    "total_statistics": {
        "sum_depth": 0, # sum of all depths among all files
        "files_processed": 0,
        "directories_processed": 0,
        "attr_dtype_freqs": {}, # attribute types
        "dset_dtype_freqs": {}, # dataset types
        "unique_attr_names": {} # unique attribute names
    },
    "attributes": {}
} 

'''
Add these statistics
- number of values
- min
- max
- range
- mean
- median
- mode
- standard deviation
'''
def add_stats_numerical(attr):
    if "values" in attr and len(attr["values"]) > 0:
        attr["num_values"] = len(attr["values"])
        min_val = attr["values"][0]
        max_val = attr["values"][0]
        sum_val = 0
        for i in range(len(attr["values"])):
            curr = attr["values"][i]
            sum_val += curr
            if curr < min_val:
                min_val = curr
            if curr > max_val:
                max_val = curr
        attr["min_value"] = min_val
        attr["max_value"] = max_val
        attr["range"] = max_val - min_val
        attr["mean"] = (sum_val)/(len(attr["values"]))
        attr["median"] = statistics.median(attr["values"])
        attr["mode"] = statistics.mode(attr["values"])
        if len(attr["values"]) > 1:
            attr["standard_deviation"] = statistics.stdev(attr["values"])


'''
Add theses statistics
- number of values
- min of string value lengths
- max of string value lengths
- range of string value lengths
- mean of string value lengths
- median of string value lengths
- mode of string value lengths
- standard deviation of string value lengths
'''
def add_stats_string(attr):
    if "values" in attr and len(attr["values"]) > 0:
        attr["num_values"] = len(attr["values"])
        str_lengths = [len(elem) for elem in attr["values"]]
        min_val = str_lengths[0]
        max_val = str_lengths[0]
        sum_val = 0
        for i in range(len(str_lengths)):
            curr = str_lengths[i]
            sum_val += curr
            if curr < min_val:
                min_val = curr
            if curr > max_val:
                max_val = curr
        attr["min_value"] = min_val
        attr["max_value"] = max_val
        attr["range"] = max_val - min_val
        attr["mean"] = (sum_val)/(len(str_lengths))
        attr["median"] = statistics.median(str_lengths)
        attr["mode"] = statistics.mode(str_lengths)
        if len(attr["values"]) > 1:
            attr["standard_deviation"] = statistics.stdev(str_lengths)


# convert all value data to serializable form
def make_serializable(data):
    for attr_key in data["attributes"].keys():
        val_type = "none"
        for i in range(len(data["attributes"][attr_key]["values"])):
            val = data["attributes"][attr_key]["values"][i]
            if isinstance(val, bytes):
                data["attributes"][attr_key]["values"][i] = val.decode('utf-8')
            elif type(val).__module__ == np.__name__:
                data["attributes"][attr_key]["values"][i] = val.item()
            val_type = type(data["attributes"][attr_key]["values"][i])
        # add additional statistics from the value data
        if val_type == int:
            data["attributes"][attr_key]["simplified_datatype"] = "int"
            add_stats_numerical(data["attributes"][attr_key])
        elif val_type == float:
            data["attributes"][attr_key]["simplified_datatype"] = "float"
            add_stats_numerical(data["attributes"][attr_key])
        elif val_type == str:
            data["attributes"][attr_key]["simplified_datatype"] = "str"
            add_stats_string(data["attributes"][attr_key])
        else:
            data["attributes"][attr_key]["simplified_datatype"] = "other"


def analyze_file_recur(f, rel_path):
    depth = 0
    for key in f[rel_path].keys(): # all content under this group
        item = f[rel_path][key]
        # process each attribute in this data_object
        for attr_key in item.attrs.keys():
            # attribute unique names
            if attr_key in data["total_statistics"]["unique_attr_names"]:
                data["total_statistics"]["unique_attr_names"][attr_key] += 1
            else:
                data["total_statistics"]["unique_attr_names"][attr_key] = 1

            attr_dtype = str(type(item.attrs[attr_key]))
            if isinstance(item.attrs[attr_key], np.ndarray):
                attr_dtype = str(item.attrs[attr_key].dtype)

            # attribute datatype frequencies
            if attr_dtype in data["total_statistics"]["attr_dtype_freqs"]:
                data["total_statistics"]["attr_dtype_freqs"][attr_dtype] += 1
            else:
                data["total_statistics"]["attr_dtype_freqs"][attr_dtype] = 1

            # each unique attribute
            if attr_key in data["attributes"]:
                if attr_dtype not in data["attributes"][attr_key]["datatype(s)"]:
                    data["attributes"][attr_key]["datatype(s)"].append(attr_dtype)
                data["attributes"][attr_key]["appearances"] += 1
                if isinstance(item.attrs[attr_key], np.ndarray):
                    data["attributes"][attr_key]["values"] = data["attributes"][attr_key]["values"] + item.attrs[attr_key].tolist()
                else:
                    data["attributes"][attr_key]["values"].append(item.attrs[attr_key])
            else:
                temp_list = []
                if isinstance(item.attrs[attr_key], np.ndarray):
                    for e in item.attrs[attr_key]:
                            temp_list.append(e)
                else:
                    temp_list = [item.attrs[attr_key]]
                data["attributes"][attr_key] = {
                    "attribute_name": attr_key,
                    "datatype(s)": [attr_dtype],
                    "appearances": 1,
                    "values": temp_list
                }

        if isinstance(f[rel_path][key], h5py.Group): # if this object is a group
            depth = max(depth, analyze_file_recur(f, rel_path+"/"+key))
        else: # this object is a dataset
            dset_dtype = str(item.dtype)
            # dataset datatype frequencies
            if dset_dtype in data["total_statistics"]["dset_dtype_freqs"]:
                data["total_statistics"]["dset_dtype_freqs"][dset_dtype] += 1
            else:
                data["total_statistics"]["dset_dtype_freqs"][dset_dtype] = 1
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
    parser = argparse.ArgumentParser('sdss_reader_json.py')
    parser.add_argument('-d', '--dir')
    args = parser.parse_args()
    return args

def main():
    args = process_console_args()

    mpi_rank = MPI.COMM_WORLD.Get_rank()
    mpi_size = MPI.COMM_WORLD.Get_size()

    traverse_dir_recur(args.dir, mpi_rank, mpi_size)

    # prints total counts of each attribute type
    data["total_statistics"]["directories_processed"]-=1 # don't count current directory
    print("\n\t-- TOTAL STATISTICS: --")
    print("# of .fits files processed:", data["total_statistics"]["files_processed"])
    print("# of directories processed:", data["total_statistics"]["directories_processed"])
    if data["total_statistics"]["files_processed"] > 0:
        print("Average File Depth:", (data["total_statistics"]["sum_depth"]/data["total_statistics"]["files_processed"]))
    print("Total Attribute Frequencies:", data["total_statistics"]["attr_dtype_freqs"])
    print("Total Dataset Frequencies:", data["total_statistics"]["dset_dtype_freqs"])
    print("Total Unique Attribute Names:", data["total_statistics"]["unique_attr_names"])

    # convert "data" dictionary to json object
    make_serializable(data)
    json_data = json.dumps(data, indent=4)
    json_out_file = open("output_"+str(mpi_rank)+".json", "w")
    json_out_file.write(json_data)
    json_out_file.close()


if __name__ == "__main__":
    main()
