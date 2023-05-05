#!/bin/bash
for WP in wp3 wp4 wp5; do
    echo
    echo eqc$WP | su $WP -c "echo; ls -lt /data/$WP/*/jupyter.log | tr -s ' ' | cut -d ' ' -f6-"
done
