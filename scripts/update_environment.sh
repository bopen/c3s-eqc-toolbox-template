#!/bin/bash

BRANCH=main

if [ $# -lt 1 ]; then
    echo "please specify the environments (i.e., 'wp3', 'wp4', and/or 'wp5')"
    exit 1
fi

create_environment ()(
    URL1="https://raw.githubusercontent.com/bopen/c3s-eqc-toolbox-template/$BRANCH/environment.yml"
    URL2="https://raw.githubusercontent.com/bopen/c3s-eqc-toolbox-template/$BRANCH/environments/environment_$1.yml"
    DATE=$(date --iso-8601=seconds)
    TMP1=$(mktemp --suffix=.yml)
    TMP2=$(mktemp --suffix=.yml)
    TMP_MERGED=$(mktemp --suffix=.yml)
    wget -O "$TMP1" "$URL1"
    wget -O "$TMP2" "$URL2"
    conda run conda-merge "$TMP1" "$TMP2" > "$TMP_MERGED"
    mamba env create -n "$1" -f "$TMP_MERGED" --yes || exit
    # mamba env update -n "$1" -f "$TMP_MERGED" --prune || exit
    mkdir -p yml_files
    conda env export -n "$1" --file yml_files/environment_"$1"_"$DATE".yml --no-build
    rm -f "$TMP1" "$TMP2" "$TMP_MERGED"
)

mamba update -n base conda mamba --yes
mamba install -c conda-forge conda-merge --yes
for WP in "$@"; do
    create_environment "$WP" || exit
done
conda clean --all --yes
