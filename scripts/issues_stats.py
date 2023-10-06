#!/usr/bin/env python3
"""Get statistics of issues."""

import pandas as pd
from github import Github

filename = input("Insert CSV file name: ")
token = input("Insert token: ")
g = Github(login_or_token=token)
repo = g.get_repo("bopen/c3s-eqc-toolbox-template")
columns = ["number", "title", "created_at", "closed_at"]
data = {column: [] for column in columns}
for issue in repo.get_issues(state="all"):
    if issue.pull_request:
        continue
    for column in columns:
        data[column].append(getattr(issue, column))
df = pd.DataFrame.from_dict(data)
df.to_csv(filename, index=False)
