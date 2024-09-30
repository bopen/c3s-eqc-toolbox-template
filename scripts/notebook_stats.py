#!/usr/bin/env python3
"""Get statistics of notebooks."""

import collections
import datetime
import pathlib
import subprocess
import tempfile

import pandas as pd

REPOSITORIES = (
    "bopen/c3s-eqc-toolbox-template",
    "ecmwf-projects/c3s2-eqc-quality-assessment",
)

data = collections.defaultdict(list)
filename = input("Insert CSV file name: ")
for repository in REPOSITORIES:
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run(
            ["git", "clone", f"git@github.com:{repository}.git", tmpdir], check=True
        )
        for path in pathlib.Path(tmpdir).glob("**/*.ipynb"):
            stdout = subprocess.run(
                f"git log --follow --format=%ad --date iso-strict -- {path!s}",
                capture_output=True,
                shell=True,
                text=True,
                cwd=tmpdir,
            ).stdout
            dates = list(map(datetime.datetime.fromisoformat, stdout.splitlines()))
            data["name"].append(str(path.relative_to(tmpdir)))
            data["repository"].append(repository)
            data["created_at"].append(dates[-1])
            data["modified_at"].append(dates[0])
df = pd.DataFrame.from_dict(data)
df.to_csv(filename, index=False)
