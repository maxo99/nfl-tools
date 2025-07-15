
import nfl_data_py as nfl

from settings import DATA_DIR


def preload(years: list[int]):
    nfl.cache_pbp(years, downcast=True, alt_path=DATA_DIR)


def get_play_by_play(years: list[int]):
    return nfl.import_pbp_data(years, downcast=True, cache=True, alt_path=DATA_DIR)
