import itertools
import os
from typing import Union

import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.flood_wave_data import FloodWaveData
from src.flood_wave_handler import FloodWaveHandler
from src.gauge_data import GaugeData
from src.graph_builder import GraphBuilder
from src.json_helper import JsonHelper
from src.measure_time import measure_time


class FloodWaveDetector:
    """This is the class responsible for finding the flood waves.

    It has all the necessary functions to find the flood waves and also has a run function which executes all the
    necessary methods in order.
    """
    def __init__(self,
                 folder_pf: str,
                 window_dict: dict,
                 delay_dict: dict,
                 centered_window_radius: int = 2,
                 gauges: Union[list, None] = None,
                 start_date: str = None,
                 end_date: str = None) -> None:
        self.data = FloodWaveData()
        self.gauges = []
        if gauges is not None:
            self.gauges = gauges
        else:
            self.gauges = self.data.gauges
        self.folder_name = f'generated_{folder_pf}'
        self.delay_dict = delay_dict
        self.window_dict = window_dict
        self.centered_window_radius = centered_window_radius
        if start_date is not None:
            self.start_date = start_date
        else:
            self.start_date = '1951-01-01'
        if end_date is not None:
            self.end_date = end_date
        else:
            self.end_date = '2020-12-31'

    @measure_time
    def run(self) -> None:
        """
        Executes the steps needed to find all the flood waves.
        :return:
        """
        self.mkdirs()
        self.find_vertices()
        self.find_edges(delay_dict=self.delay_dict, window_dict=self.window_dict, gauges=self.gauges)
        GraphBuilder().build_graph(folder_name=self.folder_name)

    @measure_time
    def find_vertices(self) -> None:
        """
        Creates a dictionary containing all the possible vertices for each station.
        The end result is saved to 'PROJECT_PATH/generated/find_vertices' folder.
        :return:
        """
        for gauge in self.gauges:
            if not os.path.exists(os.path.join(PROJECT_PATH, self.folder_name,
                                               'find_vertices', str(gauge), '.json')):
                # Get gauge data and drop missing data and make it an array.
                gauge_data = self.data.dataloader.get_daily_time_series(reg_number_list=[gauge])\
                                                 .loc[self.start_date:self.end_date].dropna()
                   
                gauge_ts = gauge_data[str(gauge)].to_numpy()
                if gauge_ts.shape[0] < (self.centered_window_radius + 1):
                    JsonHelper.write(
                        filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices', f'{gauge}.json'),
                        obj=dict()
                    )
                    print(f'No peaks found at {gauge}')
                    continue
                # Get local peak/plateau values
                local_peak_values = self.get_local_peak_values(gauge_ts=gauge_ts)

                # Create keys for dictionary
                candidate_vertices = FloodWaveDetector.find_local_maxima(
                    gauge_data=gauge_data,
                    local_peak_values=local_peak_values,
                    reg_number=str(gauge)
                )

                # Save
                JsonHelper.write(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices', f'{gauge}.json'),
                    obj=candidate_vertices
                )

    @measure_time
    def find_edges(self,
                   delay_dict: dict,
                   window_dict: dict,
                   gauges: list,
                   ) -> None:
        """
        Creates the wave-pairs for gauges next to each other.
        Creates separate jsons and a actual_next_pair (super_dict) including all the pairs with all of their waves.
        The end result is saved to 'PROJECT_PATH/generated/find_edges' folder.

        :param int delay_dict: A dictionary containing the minimum delay (days) between two gauges
        :param int window_dict: A dictionary containing the size of the interval (days) we allow a delay
        :param list gauges: The id list of the gauges (in order)
        """

        vertex_pairs = {}
        does_big_json_exist = os.path.exists(os.path.join(PROJECT_PATH, self.folder_name, 'find_edges',
                                             'vertex_pairs.json'))

        for current_gauge, next_gauge in itertools.zip_longest(gauges[:-1], gauges[1:]):
            does_actual_json_exist = os.path.exists(os.path.join(PROJECT_PATH, self.folder_name, 'find_edges',
                                                    f'{current_gauge}_{next_gauge}.json'))

            if does_actual_json_exist and does_big_json_exist:
                continue

            # Read the data from the actual gauge.
            current_gauge_candidate_vertices = FloodWaveHandler.read_vertex_file(gauge=current_gauge,
                                                                                 folder_name=self.folder_name)

            # Read the data from the next gauge.
            next_gauge_candidate_vertices = FloodWaveHandler.read_vertex_file(gauge=next_gauge,
                                                                              folder_name=self.folder_name)

            # Create actual_next_pair
            gauge_pair = dict()
            for actual_date in current_gauge_candidate_vertices['Date']:
                # Find next dates for the following gauge
                next_gauge_dates = FloodWaveHandler.find_dates_for_next_gauge(
                    actual_date=actual_date,
                    delay=delay_dict[current_gauge],
                    next_gauge_candidate_vertices=next_gauge_candidate_vertices,
                    window_size=window_dict[current_gauge]
                )

                # Convert datetime to string
                FloodWaveHandler.convert_datetime_to_str(
                    actual_date=actual_date,
                    gauge_pair=gauge_pair,
                    next_gauge_dates=next_gauge_dates
                )

            # Save to file
            JsonHelper.write(
                filepath=os.path.join(PROJECT_PATH, self.folder_name,
                                      'find_edges', f'{current_gauge}_{next_gauge}.json'),
                obj=gauge_pair
            )

            # Store result for the all in one dict
            vertex_pairs[f'{current_gauge}_{next_gauge}'] = gauge_pair

        # Save to file
        if not vertex_pairs == {}:
            JsonHelper.write(
                filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_edges', 'vertex_pairs.json'),
                obj=vertex_pairs
            )

    @measure_time
    def mkdirs(self) -> None:
        """
        Creates the 'PROJECT_PATH/generated_{folder_pf}' folder and the following 4 sub folders:
        'PROJECT_PATH/generated_{folder_pf}/find_vertices'
        'PROJECT_PATH/generated_{folder_pf}/find_edges'
        'PROJECT_PATH/generated_{folder_pf}/build_graph'
        'PROJECT_PATH/generated_{folder_pf}/new/build_graph'
        :return:
        """
        os.makedirs(os.path.join(PROJECT_PATH, self.folder_name), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, self.folder_name, 'find_edges'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, self.folder_name, 'build_graph'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, self.folder_name, 'new', 'build_graph'), exist_ok=True)

    @measure_time
    def get_local_peak_values(self, gauge_ts: np.array) -> np.array:
        """
        Finds and flags all the values from the time series which have the highest value in a 5-day centered
        time window which will be called peaks from now on, then converts the flagged timeseries to GaugeData
        :param np.array gauge_ts: the time series of a station
        :return np.array: numpy array containing the time series with the values flagged whether they are a peak or not
        """
            
        result = np.empty(gauge_ts.shape[0], dtype=GaugeData)
        cond = np.r_[np.array([True] * gauge_ts.shape[0])]

        for shift in range(1, (self.centered_window_radius + 1)):
            left_cond = np.r_[np.array([False] * shift), gauge_ts[shift:] > gauge_ts[:-shift]]
            right_cond = np.r_[gauge_ts[:-shift] >= gauge_ts[shift:], np.array([False] * shift)]
            cond = left_cond & right_cond & cond   
            
        peaks = list(np.where(cond)[0])
        
        for idx, value in enumerate(gauge_ts):
            result[idx] = GaugeData(value=value)
        for k in peaks:
            result[k].is_peak = True
        return result

    @staticmethod
    @measure_time
    def find_local_maxima(
            gauge_data: pd.DataFrame,
            local_peak_values: np.array,
            reg_number: str
            ) -> list:
        """
        Returns with the list of found (date, peak/plateau value) tuples for a single gauge

        :param pd.DataFrame gauge_data: One gauge column, one date column, date index
        :param np.array local_peak_values: Array for local peak/plateau values.
        :param str reg_number: The gauge id
        :return list: list of tuple of local max values and the date. (date, value)
        """

        # Clean-up dataframe for getting peak-plateau list
        peaks = FloodWaveHandler.clean_dataframe_for_getting_peak_list(
            local_peak_values=local_peak_values,
            gauge_data=gauge_data,
            reg_number=reg_number
        )

        # Get peak-plateau list
        return FloodWaveHandler.get_peak_list(peaks=peaks)
