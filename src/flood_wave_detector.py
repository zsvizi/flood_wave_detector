import itertools
import os

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
    def __init__(self) -> None:
        self.data = FloodWaveData()

    @measure_time
    def run(self) -> None:
        """
        Executes the steps needed to find all the flood waves.
        :return:
        """
        self.mkdirs()
        self.find_vertices()
        self.find_edges(delay=0, window_size=3, gauges=self.data.gauges)
        builder = GraphBuilder()
        builder.build_graph()

    @measure_time
    def find_vertices(self) -> None:
        """
        Creates a dictionary containing all the possible vertices for each station.
        The end result is saved to 'PROJECT_PATH/generated/find_vertices' folder.
        :return:
        """
        for gauge in self.data.gauges:
            if not os.path.exists(os.path.join(PROJECT_PATH, 'generated', 'find_vertices', str(gauge), '.json')):
                # Get gauge data and drop missing data and make it an array.
                gauge_df = self.data.dataloader.get_daily_time_series(reg_number_list=[gauge]).dropna()

                # Get local peak/plateau values
                local_peak_values = self.create_gauge_data(gauge_ts=gauge_df[str(gauge)].to_numpy())

                # Create keys for dictionary
                peak_plateau_tuples = self.create_peak_plateau_list(
                    gauge_df=gauge_df,
                    gauge_data=local_peak_values,
                    reg_number=str(gauge)
                )

                # Save
                JsonHelper.write(
                    filepath=os.path.join(PROJECT_PATH, 'generated', 'find_vertices', f'{gauge}.json'),
                    obj=peak_plateau_tuples
                )

    @measure_time
    def find_edges(self,
                   delay: int,
                   window_size: int,
                   gauges: list
                   ) -> None:
        """
        Creates the wave-pairs for gauges next to each other.
        Creates separate jsons and a actual_next_pair (super_dict) including all the pairs with all of their waves.
        The end result is saved to 'PROJECT_PATH/generated/find_edges' folder.

        :param int delay: Minimum delay (days) between two gauges
        :param int window_size: Size of the interval (days) we allow a delay
        :param list gauges: The id list of the gauges (in order)
        """

        vertex_pairs = {}
        big_json_exists = os.path.exists(os.path.join(PROJECT_PATH, 'generated', 'find_edges',
                                                      'gauge_peak_plateau_pairs.json'))

        for actual_gauge, next_gauge in itertools.zip_longest(gauges[:-1], gauges[1:]):
            actual_json_exists = os.path.exists(os.path.join(PROJECT_PATH, 'generated', 'find_edges',
                                                             f'{actual_gauge}_{next_gauge}.json'))

            if actual_json_exists and big_json_exists:
                continue

            # Read the data from the actual gauge.
            actual_gauge_df = FloodWaveHandler.read_data_from_gauge(gauge=actual_gauge)

            # Read the data from the next gauge.
            next_gauge_df = FloodWaveHandler.read_data_from_gauge(gauge=next_gauge)

            # Create actual_next_pair
            actual_next_pair = dict()
            for actual_date in actual_gauge_df['Date']:
                # Find next dates for the following gauge
                found_next_dates = FloodWaveHandler.find_dates_for_next_gauge(
                    actual_date=actual_date,
                    delay=delay,
                    next_gauge_df=next_gauge_df,
                    window_size=window_size
                )

                # Convert datetime to string
                FloodWaveHandler.convert_datetime_to_str(
                    actual_date=actual_date,
                    actual_next_pair=actual_next_pair,
                    found_next_dates=found_next_dates
                )

            # Save to file
            JsonHelper.write(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', f'{actual_gauge}_{next_gauge}.json'),
                obj=actual_next_pair
            )

            # Store result for the all in one dict
            vertex_pairs[f'{actual_gauge}_{next_gauge}'] = actual_next_pair

        # Save to file
        if not vertex_pairs == {}:
            JsonHelper.write(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', 'gauge_peak_plateau_pairs.json'),
                obj=vertex_pairs
            )

    @staticmethod
    @measure_time
    def mkdirs() -> None:
        """
        Creates the 'PROJECT_PATH/generated' folder and the following 4 sub folders:
        'PROJECT_PATH/generated/find_vertices'
        'PROJECT_PATH/generated/find_edges'
        'PROJECT_PATH/generated/build_graph'
        'PROJECT_PATH/generated/new/build_graph'
        :return:
        """
        os.makedirs(os.path.join(PROJECT_PATH, 'generated'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'find_vertices'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'find_edges'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'build_graph'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'new', 'build_graph'), exist_ok=True)

    @staticmethod
    @measure_time
    def create_gauge_data(gauge_ts: np.array) -> np.array:
        """
        Finds and flags all the values from the time series which have the highest value in a 5-day centered
        time window which will be called peaks from now on, then converts the flagged timeseries to GaugeData
        :param gauge_ts: the time series of a station
        :return: a numpy array containing the time series with the values flagged whether they are a peak or not
        """
        # TODO: refactor this function so it is possible to set window size and also not as ugly as the current version.
        result = np.empty(gauge_ts.shape[0], dtype=GaugeData)
        b = np.r_[False, False, gauge_ts[2:] > gauge_ts[:-2]]
        c = np.r_[False, gauge_ts[1:] > gauge_ts[:-1]]
        d = np.r_[gauge_ts[:-2] >= gauge_ts[2:], False, False]
        e = np.r_[gauge_ts[:-1] >= gauge_ts[1:], False]
        peak_bool = b & c & d & e
        peaks = list(np.where(peak_bool)[0])
        # print(peaks)

        for idx, value in enumerate(gauge_ts):
            result[idx] = GaugeData(value=value)
        for k in peaks:
            result[k].is_peak = True
        return result

    @staticmethod
    @measure_time
    def create_peak_plateau_list(
                                 gauge_df: pd.DataFrame,
                                 gauge_data: np.array,
                                 reg_number: str
                                 ) -> list:
        """
        Returns with the list of found (date, peak/plateau value) tuples for a single gauge

        :param pd.DataFrame gauge_df: One gauge column, one date column, date index
        :param np.array gauge_data: Array for local peak/plateau values.
        :param str reg_number: The gauge id
        :return list: list of tuple of local max values and the date. (date, value)
        """

        # Clean-up dataframe for getting peak-plateau list
        peak_plateau_df = FloodWaveHandler.clean_dataframe_for_getting_peak_plateau_list(
            gauge_data=gauge_data,
            gauge_df=gauge_df,
            reg_number=reg_number
        )

        # Get peak-plateau list
        return FloodWaveHandler.get_peak_plateau_list(peak_plateau_df=peak_plateau_df)
