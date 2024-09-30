#!/usr/bin/env python3
"""Get statistics of issues."""

import pandas as pd
from github import Github

REPOSITORIES = (
    "bopen/c3s-eqc-toolbox-template",
    "ecmwf-projects/c3s2-eqc-quality-assessment",
)

filename = input("Insert CSV file name: ")
token = input("Insert token: ")
g = Github(login_or_token=token)

columns = ["number", "title", "created_at", "closed_at"]
data = {column: [] for column in columns}
data["repository"] = []
for repo_name in REPOSITORIES:
    repo = g.get_repo(repo_name)
    for issue in repo.get_issues(state="all"):
        if issue.pull_request:
            continue
        data["repository"].append(repo_name)
        for column in columns:
            data[column].append(getattr(issue, column))
    df = pd.DataFrame.from_dict(data)
    df.to_csv(filename, index=False)
