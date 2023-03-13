
from src import PROJECT_PATH
from src.json_helper import JsonHelper

from typing import Union

import os
import pandas as pd


class Dataloader:
    def __init__(self):
        os.makedirs(os.path.join(PROJECT_PATH, 'data'), exist_ok=True)
        self.meta = self.get_metadata()
        self.data = self.read_data()




    def get_metadata(self):
        meta = pd.read_csv(os.path.join(PROJECT_PATH, 'data', 'meta.csv'), index_col=0) \
            .groupby(["river"]) \
            .get_group("Tisza") \
            .sort_values(by='river_km', ascending=False)
        return meta

    def read_data(self):
        data = pd.read_csv(os.path.join(PROJECT_PATH, 'data', '1951_2020.csv'), index_col=0)
        return data


if __name__ == "__main__":
   dl = Dataloader()
   print(dl.meta)
   print(dl.data)

