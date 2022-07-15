from copy import deepcopy
from datetime import datetime, timedelta
import itertools
import os
from queue import LifoQueue

import networkx as nx
from networkx.readwrite import json_graph
import numpy as np
import pandas as pd

from data_ativizig.dataloader import Dataloader
from src.gauge_data import GaugeData
from src.json_helper import JsonHelper
from src.measure_time import measure_time


class FloodWaveDetector:
    def __init__(self) -> None:
        self.__db_credentials_path = self.read_ini()
        self.dataloader = Dataloader(self.__db_credentials_path)
        self.meta = self.dataloader.meta_data\
                        .groupby(["river"])\
                        .get_group("Tisza")\
                        .sort_values(by='river_km', ascending=False)
        self.gauges = self.meta.dropna(subset=['h_table']).index.tolist()

        self.gauge_peak_plateau_pairs = {}
        self.gauge_pairs = []
        self.tree_g = nx.DiGraph()
        self.path = {}
        self.all_paths = {}
        self.wave_serial_number = 0
        self.branches = LifoQueue()
        self.flood_wave = {}

    @measure_time
    def run(self) -> None:
        self.mkdirs()
        self.find_vertices()
        self.find_edges()
        self.build_graph()

    @measure_time
    def find_vertices(self) -> None:
        for gauge in self.gauges:
            if not os.path.exists('saved/find_vertices/' + str(gauge) + '.json'):
                # Get gauge data and drop missing data and make it an array.
                gauge_df = self.dataloader.get_daily_time_series(reg_number_list=[gauge]).dropna()

                # Get local peak/plateau values
                local_peak_values = self.create_gauge_data_2(gauge_ts=gauge_df[str(gauge)].to_numpy())

                # Create keys for dictionary
                peak_plateau_tuples = self.create_peak_plateau_list(
                    gauge_df=gauge_df,
                    gauge_data=local_peak_values,
                    reg_number=str(gauge)
                )

                # Save
                JsonHelper.write(filepath=f'./saved/find_vertices/{gauge}.json', obj=peak_plateau_tuples)

    @measure_time
    def find_edges(self) -> None:
        self.search_flooding_gauge_pairs(delay=0, window_size=3, gauges=self.gauges)

    @measure_time
    def build_graph(self) -> None:
        """
        Searching for wave "series". For now, starting from the root ('1514-1515').
        Trying to find the same waves in different gauges.
        """

        # Read the gauge_peak_plateau_pairs (super dict)
        if self.gauge_peak_plateau_pairs == {}:
            self.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/find_edges/gauge_peak_plateau_pairs.json')

        self.gauge_pairs = list(self.gauge_peak_plateau_pairs.keys())

        """
        To understand the code better, here are some description and example:

        branches = ['1951-01-11', 3, 'path30']

            We save the branches to this stucture. It' a . 
            This means, we can get out first the element , we put in last. 
            On the above example we can see one element of branches.


        path =  {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09'}

            One path without branches.


        all_paths ={'path20': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09', 
                               '1520': '1951-01-09', '1521': '1951-01-09', '1719': '1951-01-10'}, 
                    'path30': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09'}}

            More path from the same start. The last path might be unfinished.


        flood_wave = {'id0': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-08', 
                              '1520': '1951-01-09', '1521': '1951-01-09', '1719': '1951-01-10'}, 
                      'id1': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-08'}, 
                      'id2': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09', 
                              '1520': '1951-01-09', '1521': '1951-01-09', '1719': '1951-01-10'}, 
                      'id3': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09'}}

            All of the waves from the given start point. 
        """

        for gauge_pair in self.gauge_pairs:

            root_gauge_pair_date_dict = self.gauge_peak_plateau_pairs[gauge_pair]

            os.makedirs(f'./saved/build_graph/{gauge_pair}', exist_ok=True)

            # Search waves starting from the root
            for actual_date in root_gauge_pair_date_dict.keys():

                self.reset_tree_and_flood_wave()
                # Go over every date with a wave
                for next_date in root_gauge_pair_date_dict[actual_date]:

                    # Empty and reset variables
                    self.reset_path()
                    next_g_p_idx = self.reset_gauge_pair_index_and_serial_number()

                    root_gauge = gauge_pair.split('_')[0]
                    root_gauge_next = gauge_pair.split('_')[1]

                    self.tree_g.add_edge(u_of_edge=(root_gauge, actual_date),
                                         v_of_edge=(root_gauge_next, next_date))

                    self.add_to_path(actual_date=actual_date, next_date=next_date,
                                     root_gauge=root_gauge, root_gauge_next=root_gauge_next)

                    # Search for flood wave
                    self.create_flood_wave(next_gauge_date=next_date,
                                           next_idx=next_g_p_idx)

                    # Go over the missed branches, depth search
                    self.depth_first_search()

                    # Save the wave

                    data = json_graph.node_link_data(self.tree_g)
                    JsonHelper.write(filepath=f'./saved/build_graph/{gauge_pair}/{actual_date}', obj=data)

    def add_to_path(self, actual_date: str, next_date: str, root_gauge, root_gauge_next) -> None:
        self.path[root_gauge] = actual_date
        self.path[root_gauge_next] = next_date

    def depth_first_search(self) -> None:
        while self.branches.qsize() != 0:
            # Get info from branches (info about the branch)
            new_date, new_g_p_idx, path_key = self.branches.get()
            self.path = self.all_paths[path_key]

            # Go back to the branch
            self.create_flood_wave(next_gauge_date=new_date,
                                   next_idx=new_g_p_idx)

    def reset_gauge_pair_index_and_serial_number(self) -> int:
        next_g_p_idx = 1
        self.wave_serial_number = 0
        return next_g_p_idx

    def reset_path(self) -> None:
        self.path = {}
        self.all_paths = {}

    def reset_tree_and_flood_wave(self) -> None:
        self.tree_g = nx.Graph()
        self.flood_wave = {}

    @measure_time
    def read_ini(self) -> os.path:
        dirname = os.path.dirname(os.getcwd())
        return os.path.join(dirname, 'database.ini')
    
    @measure_time
    def mkdirs(self) -> None:
        os.makedirs('./saved', exist_ok=True)
        os.makedirs('./saved/find_vertices', exist_ok=True)
        os.makedirs('./saved/find_edges', exist_ok=True)
        os.makedirs('./saved/build_graph', exist_ok=True)
        os.makedirs('./saved/new/build_graph', exist_ok=True)
    
    @measure_time
    def create_gauge_data_2(self, gauge_ts: np.array) -> np.array:
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
    
    @measure_time
    def create_peak_plateau_list(
        self,
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
        peak_plateau_df = self.clean_dataframe_for_getting_peak_plateau_list(gauge_data=gauge_data,
                                                                             gauge_df=gauge_df,
                                                                             reg_number=reg_number)

        # Get peak-plateau list
        return self.get_peak_plateau_list(peak_plateau_df)

    @staticmethod
    def get_peak_plateau_list(peak_plateau_df: pd.DataFrame) -> list:
        peak_plateau_tuple = peak_plateau_df.to_records(index=True)
        peak_plateau_list = [tuple(x) for x in peak_plateau_tuple]
        return peak_plateau_list

    @staticmethod
    def clean_dataframe_for_getting_peak_plateau_list(gauge_data: np.array, gauge_df: pd.DataFrame, reg_number: str)\
            -> pd.DataFrame:
        peak_plateau_df = gauge_df.loc[np.array([x.is_peak for x in gauge_data])]
        peak_plateau_df = peak_plateau_df.drop(columns="Date") \
            .set_index(peak_plateau_df.index.strftime('%Y-%m-%d'))
        peak_plateau_df[reg_number] = peak_plateau_df[reg_number].astype(float)
        return peak_plateau_df

    @staticmethod
    def filter_for_start_and_length(
        gauge_df: pd.DataFrame, 
        min_date: datetime, 
        window_size: int
    ) -> pd.DataFrame:
        """
        Find possible follow-up dates for the flood wave coming from the previous gauge

        :param pd.DataFrame gauge_df: Dataframe to crop
        :param datetime min_date: start date of the crop
        :param int window_size: size of the new dataframe (number of days we want)
        :return pd.DataFrame: Cropped dataframe with found next dates.
        """

        max_date = min_date + timedelta(days=window_size)
        found_next_dates = gauge_df[(gauge_df['Date'] >= min_date) & (gauge_df['Date'] <= max_date)]

        return found_next_dates
    
    @measure_time
    def search_flooding_gauge_pairs(self, delay: int, window_size: int, gauges: list) -> None:
        """
        Creates the wave-pairs for gauges next to each other. 
        Creates separate jsons and a actual_next_pair (super_dict) including all the pairs with all of their waves.

        :param int delay: Minimum delay (days) between two gauges.
        :param int window_size: Size of the interval (days) we allow a delay.
        :param list gauges: The id list of the gauges (in order).
        """
        
        gauge_peak_plateau_pairs = {}
        big_json_exists = os.path.exists('./saved/find_edges/gauge_peak_plateau_pairs.json')

        for actual_gauge, next_gauge in itertools.zip_longest(gauges[:-1], gauges[1:]):
            actual_json_exists = os.path.exists(f'saved/find_edges/{actual_gauge}_{next_gauge}.json')
            
            if actual_json_exists and big_json_exists:
                continue

            # Read the data from the actual gauge. 
            actual_gauge_df = self.read_data_from_gauge(gauge=actual_gauge)

            # Read the data from the next gauge. 
            next_gauge_df = self.read_data_from_gauge(gauge=next_gauge)

            # Create actual_next_pair
            actual_next_pair = dict()
            for actual_date in actual_gauge_df['Date']:

                # Find next dates for the following gauge
                found_next_dates = self.find_dates_for_next_gauge(actual_date=actual_date, delay=delay,
                                                                  next_gauge_df=next_gauge_df, window_size=window_size)

                # Convert datetime to string
                if not found_next_dates.empty:
                    found_next_dates_str = found_next_dates['Date'].dt.strftime('%Y-%m-%d').tolist()
                    actual_next_pair[actual_date.strftime('%Y-%m-%d')] = found_next_dates_str

            # Save to file
            JsonHelper.write(filepath=f'./saved/find_edges/{actual_gauge}_{next_gauge}.json',
                             obj=actual_next_pair)

            # Store result for the all in one dict
            gauge_peak_plateau_pairs[f'{actual_gauge}_{next_gauge}'] = actual_next_pair

        # Save to file
        if not gauge_peak_plateau_pairs == {}:
            JsonHelper.write(filepath='./saved/find_edges/gauge_peak_plateau_pairs.json', obj=gauge_peak_plateau_pairs)

    def find_dates_for_next_gauge(self, actual_date: datetime, delay: int, next_gauge_df: pd.DataFrame,
                                  window_size: int) -> pd.DataFrame:
        past_date = actual_date - timedelta(days=delay)
        found_next_dates = self.filter_for_start_and_length(
            gauge_df=next_gauge_df,
            min_date=past_date,
            window_size=window_size)
        return found_next_dates

    @staticmethod
    def read_data_from_gauge(gauge: str) -> pd.DataFrame:
        gauge_with_index = JsonHelper.read(f'./saved/find_vertices/{gauge}.json')
        gauge_df = pd.DataFrame(data=gauge_with_index,
                                columns=['Date', 'Max value'])
        gauge_df['Date'] = pd.to_datetime(gauge_df['Date'])
        return gauge_df

    def create_flood_wave(self,
                          next_gauge_date: str, next_idx: int) -> None:
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
        max_index_value = len(self.gauge_peak_plateau_pairs.keys()) - 1
        next_gauge_pair = self.gauge_pairs[next_idx]
        current_gauge = next_gauge_pair.split('_')[0]
        next_gauge = next_gauge_pair.split('_')[1]
        next_gauge_pair_date_dict = self.gauge_peak_plateau_pairs[next_gauge_pair]

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
                    self.save_info_about_branches(current_gauge=current_gauge, dat=dat, k=k,
                                                  next_gauge=next_gauge, next_gauge_date=next_gauge_date,
                                                  next_idx=next_idx)

            # Update the status of our "place" (path)
            self.update_path_status(current_gauge=current_gauge, new_gauge_date=new_gauge_date,
                                    next_gauge=next_gauge, next_gauge_date=next_gauge_date)

            # Keep going, search for the path
            self.create_flood_wave(next_gauge_date=new_gauge_date,
                                   next_idx=next_idx + 1)
        else:

            # Update the 'map'. (Add the path to the start date)
            self.flood_wave[f'id{self.wave_serial_number}'] = self.path

            # Make possible to have more paths
            self.wave_serial_number += 1

    def update_path_status(self, current_gauge: str, new_gauge_date: str,
                           next_gauge: str, next_gauge_date: str) -> None:
        self.tree_g.add_edge(u_of_edge=(current_gauge, next_gauge_date),
                             v_of_edge=(next_gauge, new_gauge_date))
        self.path[next_gauge] = new_gauge_date

    def save_info_about_branches(self, current_gauge: str, dat, k: int, next_gauge: str,
                                 next_gauge_date: str, next_idx: int) -> None:
        path_partial = deepcopy(self.path)  # copy result up to now
        self.tree_g.add_edge(u_of_edge=(current_gauge, next_gauge_date),
                             v_of_edge=(next_gauge, dat))
        path_partial[next_gauge] = dat  # update with the new node and the corresponding possible date
        new_path_key = "path" + str(next_idx + 1) + str(k)
        self.all_paths[new_path_key] = path_partial
        self.branches.put([dat, next_idx + 1, new_path_key])

    @measure_time
    def sort_wave(self, filenames: list,
                  start: str = '2006-02-01',
                  end: str = '2006-06-01') -> list:
        """
        It's hard to visualize waves far from each other. 
        With this method, we can choose a period and check the waves in it.

        :param list filenames: List of filenames we want to choose from. (Usually all files from the directory)
        :param str start: Start date of the interval.
        :param str end: Final day of the interval.
        :return str filename_sort: List of filenames with waves in the given interval.
        """
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')

        filename_sort = []

        for filename in filenames:
            date_str = filename.split(".json")[0]
            date_dt = datetime.strptime(date_str, '%Y-%m-%d')

            if start <= date_dt <= end:
                filename_sort.append(filename)

        return filename_sort
