#!/usr/bin/env python3
"""Get statistics of notebooks."""

import collections
import datetime
import pathlib
import subprocess

import pandas as pd

notebook_path = input("Insert path to notebooks dir: ")
filename = input("Insert CSV file name: ")

data = collections.defaultdict(list)
for path in pathlib.Path(notebook_path).glob("wp*/*.ipynb"):
    stdout = subprocess.run(
        f"git log --follow --format=%ad --date iso-strict {path.resolve()!s}",
        capture_output=True,
        shell=True,
        text=True,
    ).stdout
    dates = list(map(datetime.datetime.fromisoformat, stdout.splitlines()))
    data["name"].append(path.stem)
    data["wp"].append(path.parent.stem)
    data["created_at"].append(dates[-1])
    data["modified_at"].append(dates[0])
df = pd.DataFrame.from_dict(data)
df.to_csv(filename, index=False)
