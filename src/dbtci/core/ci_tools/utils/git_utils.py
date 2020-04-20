import os
import sys
import json
import subprocess
import logging
from typing import List, Dict

MAPPING = {
    "model": {"subfolder": "models", "suffix": "sql"},
    "data_test": {"subfolder": "tests", "suffix": "sql"},
    "seed": {"subfolder": "data", "suffix": "csv"},
    "macro": {"subfolder": "macros", "suffix": "sql"},
}

OBJECT_TYPES = ["model", "data_test", "seed", "macro"]


def _parse_files(project_dir, object_type, files):
    subfolder = MAPPING[object_type]["subfolder"]
    suffix = MAPPING[object_type]["suffix"]
    parsed_files = [
        os.path.splitext(file)[0]
        for file in files
        if file.startswith(f"{subfolder}/") and file.endswith(suffix)
    ]
    return [
        os.path.basename(file)
        for file in parsed_files
        if os.path.exists("{}/{}.{}".format(project_dir, file, suffix))
    ]


def fetch_changed_dbt_objects(
    project_root, object_types=OBJECT_TYPES, compare_branch="master"
):
    changed_files = subprocess.check_output(
        ["git", "diff", compare_branch, "--name-only"], cwd=project_dir
    ).split()
    changed_files = [file.decode("utf-8") for file in changed_files]
    filtered_files = {}
    for object_type in object_types:
        filtered_files[object_type] = _parse_files(
            project_dir, object_type, changed_files
        )
    return filtered_files
