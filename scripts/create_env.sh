#!/bin/bash

if [ $# -ne 1 ]; then
    echo "please specify the wp (i.e., wp3, wp4, or wp5)"
    exit 1
fi

URL1="https://raw.githubusercontent.com/bopen/c3s-eqc-toolbox-template/main/environment.yml"
URL2="https://raw.githubusercontent.com/bopen/c3s-eqc-toolbox-template/main/environments/environment_$1.yml"
DATE=$(date --iso-8601=seconds)
mamba env create -n "$1" -f "$URL1" --force || exit
mamba env update -n "$1" -f "$URL2" || exit
mkdir -p yml_files
conda env export -n "$1" --file yml_files/environment_"$1"_"$DATE".yml --no-build
