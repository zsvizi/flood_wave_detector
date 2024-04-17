import os
from datetime import datetime

import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.data.dataloader import Dataloader
from src.utils.json_helper import JsonHelper


class SlopeCalculator:
    """
    This class is responsible for calculating the slopes between given nodes that are connected by a directed
    path in the graph
    """
    def __init__(self, current_gauge: str, next_gauge: str, folder_name: str):
        """
        Constructor for SlopeCalculator class

        :param str current_gauge: station number of the current gauge as a string
        :param str next_gauge: station number of the next gauge as a string
        :param str folder_name: name of the generated data folder
        """
        self.current_vertices = None
        self.next_vertices = None
        self.distance = None
        meta = Dataloader.get_metadata()
        self.river_kms = meta["river_km"]

        self.preprocess_for_get_slopes(current_gauge=current_gauge,
                                       next_gauge=next_gauge,
                                       folder_name=folder_name)

    def preprocess_for_get_slopes(self,
                                  current_gauge: str,
                                  next_gauge: str,
                                  folder_name: str):
        """
        This is a helper function for get_slopes()
        :param str current_gauge: gauge number of current gauge as a string
        :param str next_gauge: gauge number of current gauge as a string
        :param str folder_name: name of the data folder as a string
        """
        current_vertices = JsonHelper.read(os.path.join(PROJECT_PATH, folder_name,
                                                        'find_vertices', f'{current_gauge}.json'))
        next_vertices = JsonHelper.read(os.path.join(PROJECT_PATH, folder_name,
                                                     'find_vertices', f'{next_gauge}.json'))

        current_river_km = self.river_kms[float(current_gauge)]
        next_river_km = self.river_kms[float(next_gauge)]

        distance = float(current_river_km - next_river_km)
        self.current_vertices = current_vertices
        self.next_vertices = next_vertices
        self.distance = distance

    def get_slopes(self,
                   current_date: datetime,
                   next_dates: list) -> list:
        """
        This function calculates the slopes between the current vertex and the next vertices in cm/km
        :param datetime current_date: the current date as datetime
        :param pd.DataFrame next_dates: list containing the next dates
        :return list: slopes
        """
        current_date = current_date.strftime("%Y-%m-%d")

        current_water_level = self.current_vertices[current_date][0]
        next_water_levels = []
        for next_date in next_dates:
            next_water_levels.append(self.next_vertices[next_date][0])
        next_water_levels = np.array(next_water_levels)

        level_diff = next_water_levels - current_water_level

        slopes = level_diff / self.distance

        return list(slopes)
