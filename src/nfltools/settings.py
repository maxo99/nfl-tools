import os
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.resolve()
DATA_DIR = os.path.join(ROOT_DIR, "data")
BDB_INPUT_DIR = Path("~/.kaggle/input").expanduser().resolve()
BDB_2024_DIR = os.path.abspath(f"{BDB_INPUT_DIR}/nfl-big-data-bowl-2024")
BDB_2025_DIR = os.path.abspath(f"{BDB_INPUT_DIR}/nfl-big-data-bowl-2025")

