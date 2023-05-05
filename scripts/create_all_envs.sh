#!/bin/bash
mamba update -n base conda mamba --yes
for WP in wp3 wp4 wp5; do
    ./create_env.sh $WP || exit
done
mamba clean --all --yes
