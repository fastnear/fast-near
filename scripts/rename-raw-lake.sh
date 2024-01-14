#!/bin/sh

# Move raw lake compressed files to subdirectories based on their name
# e.g. 000108000995.tgz -> 000108/000/000108000995.tgz

# Usage: rename-raw-lake.sh <raw-lake-path>

# Example: rename-raw-lake.sh ./lake-data-compressed/near-lake-data-mainnet/0

data_path=$1

for file in $data_path/*.tgz; do
    file_name=$(basename $file)
    dir=$(echo $file_name | sed -E 's/([0-9]{6})([0-9]{3})([0-9]{3}).tgz/\1\/\2/')

    mkdir -p $data_path/$dir
    mv $file $data_path/$dir
    echo $dir/$file_name
done