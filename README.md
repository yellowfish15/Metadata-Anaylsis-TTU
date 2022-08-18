# Metadata-Anaylsis-TTU

## Description
Collection of code written in Python and Shell Script used to extract metadata from the SDSS database

Created as a part of the Anson L. Clark Scholars Summer Program at Texas Tech University

These scripts were run and tested on the HPCC at Texas Tech to analyze metadata found within data from the SDSS

## Usage
Extracted metadata is stored in `.tsv` and `.json` format, which have extraction code and sample data stored in their respective folders ("json/" and "tsv/")

The `tsv/sdss_reader_sv.py` file converts the raw fits data files into hdf5 files and extracts metadata attributes from this data into tab separated value format

The `json/sdss_reader_json.py` does the same extraction process, except it stores the data into JSON format
