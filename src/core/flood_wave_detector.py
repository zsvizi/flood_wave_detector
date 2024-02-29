import copy
import itertools
import json
import os
from datetime import datetime, timedelta
from typing import Union

import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.data.flood_wave_data import FloodWaveData
from src.core.flood_wave_handler import FloodWaveHandler
from src.data.gauge_data import GaugeData
from src.core.graph_builder import GraphBuilder
from src.utils.json_helper import JsonHelper
from src.utils.measure_time import measure_time


class FloodWaveDetector:
    """This is the class responsible for finding the flood waves.

    It has all the necessary functions to find the flood waves and also has a run function which executes all the
    necessary methods in order.
    """
    def __init__(self,
                 folder_pf: str,
                 forward_dict: dict,
                 backward_dict: dict,
                 centered_window_radius: int = 2,
                 gauges: Union[list, None] = None,
                 start_date: str = None,
                 end_date: str = None) -> None:
        """
        Constructor for FloodWaveDetector class

        :param str folder_pf: The name of the to be generated folder, which will contain the generated files.
        :param dict forward_dict: The dictionary containing the number of days allowed after a node for continuation,
                                  for each gauge. This parameter is also called as beta.
        :param dict backward_dict: The dictionary containing the number of days allowed before a node for continuation,
                                   for each gauge. This parameter is also called as alpha.
        :param int centered_window_radius: The number of days that a record of time series is required to be greater
                                           than the records before and to be greater or equal to after, to be considered
                                           as a peak. (I.e.: centered_window_radius=2 means that you need 2 smaller
                                            values before and 2 nom-greater values after)
        :param Union[list, None] gauges: The gauges used for the analysis.
        :param str start_date: The date to start the flood wave search from.
        :param str start_date: The date to finish the flood wave search at.
        """
        
        self.data = FloodWaveData()
        self.gauges = []
        if gauges is not None:
            self.gauges = gauges
        else:
            self.gauges = self.data.gauges
        self.folder_name = f'generated_{folder_pf}'
        self.backward_dict = backward_dict
        self.forward_dict = forward_dict
        self.centered_window_radius = centered_window_radius
        if start_date is not None:
            self.start_date = start_date
        else:
            self.start_date = '1876-01-01'
        if end_date is not None:
            self.end_date = end_date
        else:
            self.end_date = '2019-12-31'

    @measure_time
    def run(self) -> None:
        """
        Executes the steps needed to find all the flood waves.
        """
        self.mkdirs()

        gauges_copy = copy.deepcopy(self.gauges)

        stations_life_intervals = JsonHelper.read(filepath=os.path.join(PROJECT_PATH,
                                                                        'data', 'existing_stations.json'))

        cut_dates = FloodWaveHandler.get_dates_in_between(start_date=self.start_date,
                                                          end_date=self.end_date,
                                                          intervals=stations_life_intervals,
                                                          gauges=self.gauges)

        for i in range(len(cut_dates) - 1):
            self.start_date = cut_dates[i]
            self.end_date = cut_dates[i + 1]

            exist = str(datetime.strptime(cut_dates[i], "%Y-%m-%d") + timedelta(days=1))
            existing_gauges = []
            for gauge in gauges_copy:
                if stations_life_intervals[str(gauge)]["start"] <= exist <= stations_life_intervals[str(gauge)]["end"]:
                    existing_gauges.append(gauge)
            self.gauges = existing_gauges

            self.find_vertices()
            self.find_edges()

        # Set original values
        self.gauges = gauges_copy
        self.start_date = cut_dates[0]
        self.end_date = cut_dates[-1]

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
                gauge_data = self.data.data[[str(gauge), 'Date']]\
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
                candidate_vertices = self.find_local_maxima(
                    gauge_data=gauge_data,
                    local_peak_values=local_peak_values,
                    reg_number=str(gauge)
                )

                # Save
                if not os.path.exists(os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices', f'{gauge}.json')):
                    JsonHelper.write(
                        filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices', f'{gauge}.json'),
                        obj=candidate_vertices)
                else:
                    candidate_read = JsonHelper.read(
                        filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices', f'{gauge}.json'))

                    candidate_read.update(candidate_vertices)
                    JsonHelper.write(
                        filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_vertices', f'{gauge}.json'),
                        obj=candidate_read)

    @measure_time
    def find_edges(self) -> None:
        """
        Creates the wave-pairs for gauges next to each other.
        Creates separate jsons and an actual_next_pair (super_dict) including all the pairs with all of their waves.
        The end result is saved to 'PROJECT_PATH/generated/find_edges' folder.
        """

        river_kms = self.data.meta["river_km"]
        vertex_pairs = {}
        for current_gauge, next_gauge in itertools.zip_longest(self.gauges[:-1], self.gauges[1:]):
            # Read the data from the actual gauge.
            current_gauge_candidate_vertices = FloodWaveHandler.read_vertex_file(gauge=current_gauge,
                                                                                 folder_name=self.folder_name)

            # Read the data from the next gauge.
            next_gauge_candidate_vertices = FloodWaveHandler.read_vertex_file(gauge=next_gauge,
                                                                              folder_name=self.folder_name)

            current_vertices, next_vertices, current_null, next_null, distance = \
                FloodWaveHandler.preprocess_for_get_slopes(current_gauge=str(current_gauge),
                                                           next_gauge=str(next_gauge),
                                                           river_kms=river_kms,
                                                           folder_name=self.folder_name)

            # Create actual_next_pair
            gauge_pair = dict()
            for actual_date in current_gauge_candidate_vertices['Date']:
                # Find next dates for the following gauge
                next_gauge_dates = FloodWaveHandler.find_dates_for_next_gauge(
                    actual_date=actual_date,
                    backward=self.backward_dict[current_gauge],
                    next_gauge_candidate_vertices=next_gauge_candidate_vertices,
                    forward=self.forward_dict[current_gauge]
                )

                slopes = FloodWaveHandler.get_slopes(current_date=actual_date,
                                                     next_dates=next_gauge_dates,
                                                     current_vertices=current_vertices,
                                                     next_vertices=next_vertices,
                                                     current_null=current_null,
                                                     next_null=next_null,
                                                     distance=distance)

                # Convert datetime to string
                FloodWaveHandler.convert_datetime_to_str(
                    actual_date=actual_date,
                    gauge_pair=gauge_pair,
                    next_gauge_dates=next_gauge_dates,
                    slopes=slopes
                )

            # Save to file
            if not os.path.exists(os.path.join(PROJECT_PATH, self.folder_name,
                                  'find_edges', f'{current_gauge}_{next_gauge}.json')):
                JsonHelper.write(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name,
                                          'find_edges', f'{current_gauge}_{next_gauge}.json'),
                    obj=gauge_pair)
            else:
                gauge_pair_read = JsonHelper.read(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name,
                                          'find_edges', f'{current_gauge}_{next_gauge}.json'))

                gauge_pair_read.update(gauge_pair)
                JsonHelper.write(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name,
                                          'find_edges', f'{current_gauge}_{next_gauge}.json'),
                    obj=gauge_pair_read)

            # Store result for the all-in-one dict
            vertex_pairs[f'{current_gauge}_{next_gauge}'] = gauge_pair

        # Save to file
        if not vertex_pairs == {}:
            if not os.path.exists(os.path.join(PROJECT_PATH, self.folder_name, 'find_edges', 'vertex_pairs.json')):
                JsonHelper.write(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_edges', 'vertex_pairs.json'),
                    obj=vertex_pairs)
            else:
                vertex_pairs_read = JsonHelper.read(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_edges', 'vertex_pairs.json'))
                vertex_pairs_read.update(vertex_pairs)
                JsonHelper.write(
                    filepath=os.path.join(PROJECT_PATH, self.folder_name, 'find_edges', 'vertex_pairs.json'),
                    obj=vertex_pairs_read)

    @measure_time
    def mkdirs(self) -> None:
        """
        Creates the 'PROJECT_PATH/generated_{folder_pf}' folder and the following 4 sub folders:
        'PROJECT_PATH/generated_{folder_pf}/find_vertices'
        'PROJECT_PATH/generated_{folder_pf}/find_edges'
        'PROJECT_PATH/generated_{folder_pf}/build_graph'
        'PROJECT_PATH/generated_{folder_pf}/new/build_graph'
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
        time window which will be called peaks from now on, then converts the flagged time series to GaugeData
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
            ) -> dict:
        """
        Returns with the list of found (date, peak/plateau value) tuples for a single gauge

        :param pd.DataFrame gauge_data: One gauge column, one date column, date index
        :param np.array local_peak_values: Array for local peak/plateau values.
        :param str reg_number: The gauge id
        :return dict: dictionary of tuple of local max values and the date. (date: [value, color])
        """

        g = open(os.path.join(PROJECT_PATH, "data", "level_groups_fontos.json"))
        level_groups = json.load(g)
        level_group = level_groups[reg_number]

        # Clean-up dataframe for getting peak-plateau list
        peaks = FloodWaveHandler.clean_dataframe_for_getting_peak_list(
            local_peak_values=local_peak_values,
            gauge_data=gauge_data,
            reg_number=reg_number
        )

        # Get peak-plateau list
        return FloodWaveHandler.get_peak_list(peaks=peaks, level_group=level_group)
