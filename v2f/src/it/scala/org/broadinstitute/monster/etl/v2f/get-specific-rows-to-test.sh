#!/usr/bin/env bash

# for every created file, get the top and bottom n rows (by line length) to account for columns with missing data
# then create a new file with just those rows. This way, we can run stuff through the pipeline and get outputs that
# are human readable (as in they are a manageable number of rows).
gsutil ls gs://broad-dsp-monster-v2f-dev-ingest-storage/test/inputs/** > filenames.txt

# make a directory to store the files in
mkdir test-files

# TODO make rest of directory structure for inputs


# first put the header of each file in
while read filename; do
gsutil cat "$filename" | awk 'NR==1{ print $0 }' > test-files/${filename#"gs://broad-dsp-monster-v2f-dev-ingest-storage/test/inputs"*/}; done <filenames.txt

# then append the 10 lines with the smallest counts
while read filename; do
gsutil cat "$filename" | awk 'NR>1{ print length, $0 }' | sort -n | cut -d" " -f2- | head -n 10 >> test-files/${filename#"gs://broad-dsp-monster-v2f-dev-ingest-storage/test/inputs"*/}; done <filenames.txt

# then append the 10 lines with the greatest counts
while read filename; do
gsutil cat "$filename" | awk 'NR>1{ print length, $0 }' | sort -n | cut -d" " -f2- | tail -n 10 >> test-files/${filename#"gs://broad-dsp-monster-v2f-dev-ingest-storage/test/inputs"*/}; done <filenames.txt

# TODO delete filenames.txt

# TODO add empty .csv files