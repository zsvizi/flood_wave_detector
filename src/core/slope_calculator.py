import os
from datetime import datetime

import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.utils.json_helper import JsonHelper


class SlopeCalculator:
    def __init__(self, current_gauge: str, next_gauge: str, river_kms: pd.DataFrame, folder_name: str):
        self.current_vertices = None
        self.next_vertices = None
        self.current_null = None
        self.next_null = None
        self.distance = None
        self.preprocess_for_get_slopes(current_gauge=current_gauge,
                                       next_gauge=next_gauge,
                                       river_kms=river_kms,
                                       folder_name=folder_name)

    def preprocess_for_get_slopes(self,
                                  current_gauge: str,
                                  next_gauge: str,
                                  river_kms: pd.DataFrame,
                                  folder_name: str):
        """
        This is a helper function for get_slopes()
        :param str current_gauge: gauge number of current gauge as a string
        :param str next_gauge: gauge number of current gauge as a string
        :param pd.DataFrame river_kms: dataframe of river kilometers
        :param str folder_name: name of the data folder as a string
        """
        null_points = JsonHelper.read(os.path.join(PROJECT_PATH, 'data', 'nullpontok_fontos.json'))
        current_null = null_points[current_gauge]
        next_null = null_points[next_gauge]

        current_vertices = JsonHelper.read(os.path.join(PROJECT_PATH, folder_name,
                                                        'find_vertices', f'{current_gauge}.json'))
        next_vertices = JsonHelper.read(os.path.join(PROJECT_PATH, folder_name,
                                                     'find_vertices', f'{next_gauge}.json'))

        current_river_km = river_kms[float(current_gauge)]
        next_river_km = river_kms[float(next_gauge)]

        distance = float(next_river_km - current_river_km)
        self.current_vertices = current_vertices
        self.next_vertices = next_vertices
        self.current_null = current_null
        self.next_null = next_null
        self.distance = distance

    def get_slopes(self,
                   current_date: datetime,
                   next_dates: pd.DataFrame) -> list:
        """
        This function calculates the slopes between the current vertex and the next vertices in cm/km
        :param datetime current_date: the current date as datetime
        :param pd.DataFrame next_dates: dataframe containing the next dates
        :return list: slopes
        """
        current_date = current_date.strftime("%Y-%m-%d")

        current_water_level = self.current_vertices[current_date][0]
        next_water_levels = []
        next_dates_str = next_dates['Date'].dt.strftime('%Y-%m-%d').tolist()
        for next_date in next_dates_str:
            next_water_levels.append(self.next_vertices[next_date][0])
        next_water_levels = np.array(next_water_levels)

        level_diff = (next_water_levels + float(self.next_null)) - (current_water_level + float(self.current_null))

        slopes = level_diff / self.distance

        return list(slopes)
