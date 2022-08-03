from copy import deepcopy
import itertools
import os

import networkx as nx
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.flood_wave_data import FloodWaveData
from src.flood_wave_handler import FloodWaveHandler
from src.gauge_data import GaugeData
from src.json_helper import JsonHelper
from src.measure_time import measure_time


class FloodWaveDetector:
    def __init__(self) -> None:
        self.data = FloodWaveData()

    @measure_time
    def run(self) -> None:
        self.mkdirs()
        self.find_vertices()
        self.find_edges(delay=0, window_size=3, gauges=self.data.gauges)
        self.build_graph()

    @measure_time
    def find_vertices(self) -> None:
        for gauge in self.data.gauges:
            if not os.path.exists(os.path.join(PROJECT_PATH, 'generated', 'find_vertices', str(gauge), '.json')):
                # Get gauge data and drop missing data and make it an array.
                gauge_df = self.data.dataloader.get_daily_time_series(reg_number_list=[gauge]).dropna()

                # Get local peak/plateau values
                local_peak_values = self.create_gauge_data_2(gauge_ts=gauge_df[str(gauge)].to_numpy())

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

        :param int delay: Minimum delay (days) between two gauges.
        :param int window_size: Size of the interval (days) we allow a delay.
        :param list gauges: The id list of the gauges (in order).
        """

        gauge_peak_plateau_pairs = {}
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
            gauge_peak_plateau_pairs[f'{actual_gauge}_{next_gauge}'] = actual_next_pair

        # Save to file
        if not gauge_peak_plateau_pairs == {}:
            JsonHelper.write(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', 'gauge_peak_plateau_pairs.json'),
                obj=gauge_peak_plateau_pairs
            )

    @measure_time
    def build_graph(self) -> None:
        """
        Searching for wave "series". For now, starting from the root ('1514-1515').
        Trying to find the same waves in different gauges.
        """

        # Read the gauge_peak_plateau_pairs (super dict)
        if self.data.gauge_peak_plateau_pairs == {}:
            self.data.gauge_peak_plateau_pairs = JsonHelper.read(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', 'gauge_peak_plateau_pairs.json')
            )

        self.data.gauge_pairs = list(self.data.gauge_peak_plateau_pairs.keys())

        for gauge_pair in self.data.gauge_pairs:

            root_gauge_pair_date_dict = self.data.gauge_peak_plateau_pairs[gauge_pair]

            os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'build_graph', f'{gauge_pair}'), exist_ok=True)

            # Search waves starting from the root
            for actual_date in root_gauge_pair_date_dict.keys():

                self.reset_tree_and_flood_wave()
                # Go over every date with a wave
                for next_date in root_gauge_pair_date_dict[actual_date]:

                    # Empty and reset variables
                    next_g_p_idx = self.reset_gauge_pair_index_and_serial_number()

                    self.add_to_graph(actual_date=actual_date,
                                      gauge_pair=gauge_pair,
                                      next_date=next_date)

                    # Search for flood wave
                    self.create_flood_wave(
                        next_gauge_date=next_date,
                        next_idx=next_g_p_idx
                    )

                    # Go over the missed branches, depth search
                    self.depth_first_search()

                    # Save the wave

                    data = nx.readwrite.json_graph.node_link_data(self.data.tree_g)
                    JsonHelper.write(
                        filepath=os.path.join(PROJECT_PATH, 'generated', 'build_graph', f'{gauge_pair}/{actual_date}'),
                        obj=data
                    )

    def depth_first_search(self) -> None:
        while self.data.branches.qsize() != 0:
            # Get info from branches (info about the branch)
            new_date, new_g_p_idx, path_key = self.data.branches.get()
            self.data.path = self.data.all_paths[path_key]

            # Go back to the branch
            self.create_flood_wave(
                next_gauge_date=new_date,
                next_idx=new_g_p_idx
            )

    def create_flood_wave(self,
                          next_gauge_date: str,
                          next_idx: int
                          ) -> None:
        """
        Recursive function walking along the paths in the rooted tree representing the flood wave
        We assume that global variable path contains the complete path up to the current state
        i.e. all nodes (=gauges) are stored before the call of create_flood_wave

        :param str next_gauge_date: The next date, we want to find in the next pair's json.
        A date from the list, not the key. Date after the branch
        :param int next_idx: Index of the next gauge pair.
        E.g: index 1 is referring to "1515-1516" if the root is "1514-1515".
        """

        # other variables
        max_index_value = len(self.data.gauge_peak_plateau_pairs.keys()) - 1
        next_gauge_pair = self.data.gauge_pairs[next_idx]
        current_gauge = next_gauge_pair.split('_')[0]
        next_gauge = next_gauge_pair.split('_')[1]
        next_gauge_pair_date_dict = self.data.gauge_peak_plateau_pairs[next_gauge_pair]

        # See if we continue the wave
        can_path_be_continued = next_gauge_date in next_gauge_pair_date_dict.keys()

        if can_path_be_continued and next_idx < max_index_value:

            # Get new data values
            new_date_value = next_gauge_pair_date_dict[next_gauge_date]
            # the recursion continues with the first date
            new_gauge_date = new_date_value[0]

            # we store the other possible dates for continuation in a LiFoQueue
            if len(new_date_value) > 1:

                # Save the informations about the branches in a LiFoQueue (branches) so we can come back later.
                for k, dat in enumerate(new_date_value[1:]):
                    self.save_info_about_branches(
                        current_gauge=current_gauge,
                        dat=dat,
                        k=k,
                        next_gauge=next_gauge,
                        next_gauge_date=next_gauge_date,
                        next_idx=next_idx
                    )

            # Update the status of our "place" (path)
            self.update_path_status(
                current_gauge=current_gauge,
                new_gauge_date=new_gauge_date,
                next_gauge=next_gauge,
                next_gauge_date=next_gauge_date
            )

            # Keep going, search for the path
            self.create_flood_wave(
                next_gauge_date=new_gauge_date,
                next_idx=next_idx + 1
            )
        else:

            # Update the 'map'. (Add the path to the start date)
            self.data.flood_wave[f'id{self.data.wave_serial_number}'] = self.data.path

            # Make possible to have more paths
            self.data.wave_serial_number += 1

    def add_to_graph(self,
                     actual_date: str,
                     gauge_pair: str,
                     next_date: str
                     ) -> None:

        self.reset_path()

        root_gauge = gauge_pair.split('_')[0]
        root_gauge_next = gauge_pair.split('_')[1]

        self.data.tree_g.add_edge(
            u_of_edge=(root_gauge, actual_date),
            v_of_edge=(root_gauge_next, next_date)
        )

        self.add_to_path(
            actual_date=actual_date,
            next_date=next_date,
            root_gauge=root_gauge,
            root_gauge_next=root_gauge_next
        )

    def add_to_path(self,
                    actual_date: str,
                    next_date: str,
                    root_gauge: str,
                    root_gauge_next: str
                    ) -> None:
        self.data.path[root_gauge] = actual_date
        self.data.path[root_gauge_next] = next_date

    def reset_gauge_pair_index_and_serial_number(self) -> int:
        next_g_p_idx = 1
        self.data.wave_serial_number = 0
        return next_g_p_idx

    def reset_path(self) -> None:
        self.data.path = {}
        self.data.all_paths = {}

    def reset_tree_and_flood_wave(self) -> None:
        self.data.tree_g = nx.Graph()
        self.data.flood_wave = {}

    @staticmethod
    @measure_time
    def mkdirs() -> None:
        os.makedirs(os.path.join(PROJECT_PATH, 'generated'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'find_vertices'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'find_edges'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'build_graph'), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'new', 'build_graph'), exist_ok=True)

    def update_path_status(self,
                           current_gauge: str,
                           new_gauge_date: str,
                           next_gauge: str,
                           next_gauge_date: str
                           ) -> None:

        self.data.tree_g.add_edge(
            u_of_edge=(current_gauge, next_gauge_date),
            v_of_edge=(next_gauge, new_gauge_date)
        )
        self.data.path[next_gauge] = new_gauge_date

    def save_info_about_branches(self,
                                 current_gauge: str,
                                 dat: str,
                                 k: int,
                                 next_gauge: str,
                                 next_gauge_date: str,
                                 next_idx: int
                                 ) -> None:

        path_partial = deepcopy(self.data.path)  # copy result up to now
        self.data.tree_g.add_edge(
            u_of_edge=(current_gauge, next_gauge_date),
            v_of_edge=(next_gauge, dat)
        )
        path_partial[next_gauge] = dat  # update with the new node and the corresponding possible date
        new_path_key = "path" + str(next_idx + 1) + str(k)
        self.data.all_paths[new_path_key] = path_partial
        self.data.branches.put([dat, next_idx + 1, new_path_key])

    @staticmethod
    @measure_time
    def create_gauge_data_2(gauge_ts: np.array) -> np.array:
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

        :param pd.DataFrame gauge_df: One gauge column, one date column, date index.
        :param np.array gauge_data: Array for local peak/plateau values.
        :param str reg_number: The gauge id.
        :return list: list of tuple of local max values and the date. (date, value)
        """

        # Clean-up dataframe for getting peak-plateau list
        peak_plateau_df = FloodWaveHandler.clean_dataframe_for_getting_peak_plateau_list(
            gauge_data=gauge_data,
            gauge_df=gauge_df,
            reg_number=reg_number
        )

        # Get peak-plateau list
        return FloodWaveHandler.get_peak_plateau_list(peak_plateau_df)
