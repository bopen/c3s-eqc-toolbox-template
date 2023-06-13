#!/usr/bin/env python3
"""Start jupyter servers for EQC users on the VM."""
import getpass
import logging
import pathlib
import subprocess
import urllib.parse
from typing import Optional

logging.basicConfig(level=logging.INFO)

wd_path = pathlib.Path.cwd()
username = wd_path.name

expected_parent = pathlib.Path(f"/data/{getpass.getuser()}")
if wd_path.parent != expected_parent or len(username.split("_")) != 2:
    expected_dir = expected_parent / "lastname_firstname"
    raise ValueError(f"Working directory must be {expected_dir}")


def get_jupyter_server_url(wd_path: pathlib.Path) -> Optional[urllib.parse.ParseResult]:
    cmd = ["jupyter", "lab", "list"]
    proc = subprocess.run(cmd, capture_output=True, check=True, text=True)
    for line in proc.stdout.splitlines()[1:]:
        jupyter_url, jupyter_dir = [segment.strip() for segment in line.split("::")]
        if pathlib.Path(jupyter_dir) == wd_path:
            return urllib.parse.urlparse(jupyter_url)
    return None


def start_jupyter_server(wd_path: pathlib.Path) -> urllib.parse.ParseResult:
    log = wd_path / "jupyter.log"
    cmd = f"jupyter lab --no-browser > {log!s} 2>&1 &"
    subprocess.run(cmd, check=True, shell=True)
    while True:
        jupyter_url = get_jupyter_server_url(wd_path)
        if jupyter_url:
            return jupyter_url


jupyter_url = get_jupyter_server_url(wd_path) or start_jupyter_server(wd_path)

msg = f"""Serving notebooks from local directory: {wd_path}
Jupyter Notebook is running at:
{jupyter_url.geturl()}

To access the notebooks:
    1. Go to your local machine and paste this command:
            ssh -N -f -L localhost:5678:{jupyter_url.netloc} eqcuser@136.156.154.152
    2. Open a browser and paste this URL:
            http://localhost:5678/?{jupyter_url.query}

To stop the server use this command:
    jupyter lab stop {jupyter_url.netloc.split(":")[-1]}
"""
logging.info(msg)
