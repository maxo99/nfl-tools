import os
from pathlib import Path

import git

ROOT_DIR = str(git.Repo(".", search_parent_directories=True).working_tree_dir)
DATA_DIR = os.path.join(ROOT_DIR, "data")

# Big Data Bowl dataset paths
BDB_INPUT_DIR = Path("~/.kaggle/input").expanduser().resolve()
BDB_2024_DIR = os.path.abspath(f"{BDB_INPUT_DIR}/nfl-big-data-bowl-2024")
BDB_2025_DIR = os.path.abspath(f"{BDB_INPUT_DIR}/nfl-big-data-bowl-2025")
