from copy import deepcopy
from datetime import datetime, timedelta
from functools import wraps
import itertools
import json
import os
from queue import LifoQueue
from time import time
from typing import Union

import matplotlib.pyplot as plt
import networkx as nx
from networkx.readwrite import json_graph
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

from data_ativizig.dataloader import Dataloader
from src.gauge_data import GaugeData
from src.json_helper import JsonHelper
from src.measure_time import measure_time

class FloodWaveDetector():
    def __init__(self) -> None:
        self.__db_credentials_path = self.read_ini()
        self.dataloader = Dataloader(self.__db_credentials_path)
        self.meta = self.dataloader\
                        .meta_data\
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
    def read_ini(self) -> os.path:
        dirname = os.path.dirname(os.getcwd())
        return os.path.join(dirname, 'database.ini')
    
    @measure_time
    def mkdirs(self):
        os.makedirs('./saved', exist_ok = True)
        os.makedirs('./saved/step1', exist_ok = True)
        os.makedirs('./saved/step2', exist_ok = True)
        os.makedirs('./saved/step3', exist_ok = True)
        os.makedirs('./saved/new/step3', exist_ok = True)
    
    @measure_time
    def create_gauge_data(self, gauge_ts: np.array) -> np.array:
        result = np.empty(gauge_ts.shape[0], dtype=GaugeData)
        peaks = list(argrelextrema(gauge_ts, np.greater_equal)[0])
        
        
        for idx, value in enumerate(gauge_ts):
            result[idx] = GaugeData(value=value)
        for k in peaks:
            result[k].is_peak = True
        return result
    
    @measure_time
    def create_gauge_data_2(self, gauge_ts: np.array) -> np.array:
        result = np.empty(gauge_ts.shape[0], dtype=GaugeData)
        b = np.r_[False, False, gauge_ts[2:] > gauge_ts[:-2]]
        c = np.r_[False, gauge_ts[1:] > gauge_ts[:-1]]
        d = np.r_[gauge_ts[:-2] >= gauge_ts[2:], False, False]
        e = np.r_[gauge_ts[:-1] >= gauge_ts[1:], False]
        peak_bool = b & c & d & e
        peaks = list(np.where(peak_bool)[0])
        #print(peaks)
        
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
        :param np.array bool_filter: A bool list. There is a local peak/plateau, where it's true.
        :param str reg_number: The gauge id.
        :return list: list of tuple of local max values and the date. (date, value) 
        """

        # Clean-up dataframe for getting peak-plateau list
        peak_plateau_df = gauge_df.loc[np.array([x.is_peak for x in gauge_data])]
        peak_plateau_df = peak_plateau_df.set_index(peak_plateau_df.index.strftime('%Y-%m-%d'))
        peak_plateau_df[reg_number] = peak_plateau_df[reg_number].astype(float)

        # Get peak-plateau list
        peak_plateau_tuple = peak_plateau_df.to_records(index=True)
        peak_plateau_list = [tuple(x) for x in peak_plateau_tuple]
        return peak_plateau_list
    

    def filter_for_start_and_length(
        self,
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
        big_json_exists = os.path.exists('./saved/step2/gauge_peak_plateau_pairs.json')

        for actual_gauge, next_gauge in itertools.zip_longest(gauges[:-1], gauges[1:]):
            actual_json_exists = os.path.exists(f'saved/step2/{actual_gauge}_{next_gauge}.json')
            
            if actual_json_exists and big_json_exists:
                continue

            # Read the data from the actual gauge. 
            actual_gauge_with_index = JsonHelper.read(f'./saved/step1/{actual_gauge}.json')
            actual_gauge_df = pd.DataFrame(data=actual_gauge_with_index, 
                                           columns=['Date', 'Max value'])
            actual_gauge_df['Date'] = pd.to_datetime(actual_gauge_df['Date'])

            # Read the data from the next gauge. 
            next_gauge_with_index = JsonHelper.read(f'./saved/step1/{next_gauge}.json')
            next_gauge_df = pd.DataFrame(data=next_gauge_with_index, 
                                         columns=['Date', 'Max value'])
            next_gauge_df['Date'] = pd.to_datetime(next_gauge_df['Date'])

            # Create actual_next_pair
            actual_next_pair = dict()
            for actual_date in actual_gauge_df['Date']:

                # Find next dates for the following gauge
                past_date = actual_date - timedelta(days=delay)
                found_next_dates = self.filter_for_start_and_length(
                    gauge_df=next_gauge_df, 
                    min_date=past_date, 
                    window_size=window_size)

                # Convert datetime to string
                if not found_next_dates.empty:
                    found_next_dates_str = found_next_dates['Date'].dt.strftime('%Y-%m-%d').tolist()
                    actual_next_pair[actual_date.strftime('%Y-%m-%d')] = found_next_dates_str

            # Save to file
            JsonHelper.write(filepath=f'./saved/step2/{actual_gauge}_{next_gauge}.json',
                             obj=actual_next_pair)

            # Store result for the all in one dict
            gauge_peak_plateau_pairs[f'{actual_gauge}_{next_gauge}'] = actual_next_pair

        # Save to file
        if not gauge_peak_plateau_pairs == {}:
            JsonHelper.write(filepath='./saved/step2/gauge_peak_plateau_pairs.json', obj=gauge_peak_plateau_pairs)
    
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
                    path_partial = deepcopy(self.path)  # copy result up to now
                    self.tree_g.add_edge(u_of_edge=(current_gauge, next_gauge_date),
                                         v_of_edge=(next_gauge, dat))
                    path_partial[next_gauge] = dat  # update with the new node and the corresponding possible date
                    new_path_key = "path" + str(next_idx + 1) + str(k)
                    self.all_paths[new_path_key] = path_partial 
                    self.branches.put([dat , next_idx + 1, new_path_key])

            # Update the status of our "place" (path)
            self.tree_g.add_edge(u_of_edge=(current_gauge, next_gauge_date),
                                 v_of_edge=(next_gauge, new_gauge_date))
            self.path[next_gauge] = new_gauge_date

            # Keep going, search for the path
            self.create_flood_wave(next_gauge_date=new_gauge_date, 
                              next_idx=next_idx + 1)
        else:

            # Update the 'map'. (Add the path to the start date)
            self.flood_wave[f'id{self.wave_serial_number}'] = self.path

            # Make possible to have more paths
            self.wave_serial_number += 1
            
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

            if date_dt >= start and date_dt <= end:
                filename_sort.append(filename)

        return filename_sort
    
    def filter_graph(self, start_station: int, end_station: int, start_date: str, end_date: str):
        
        if self.gauge_peak_plateau_pairs == {}:            
            self.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/step2/gauge_peak_plateau_pairs.json', log=False)
            
        # first filter
        self.gauge_pairs = list(self.gauge_peak_plateau_pairs.keys())
        up_limit = self.meta.loc[start_station].river_km
        low_limit = self.meta.loc[end_station].river_km
        selected_meta = self.meta[(self.meta['river_km'] >= low_limit)]
        start_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        
        selected_pairs = [x for x in self.gauge_pairs if int(x.split('_')[0]) in start_gauges ]
        
        joined_graph = nx.Graph()
        for gauge_pair in selected_pairs:
            filenames = next(os.walk(f'./saved/step3/{gauge_pair}'), (None, None, []))[2]
            sorted_files = self.sort_wave(filenames=filenames, start=start_date, end=end_date)
            for file in sorted_files:
                data = JsonHelper.read(filepath=f'./saved/step3/{gauge_pair}/{file}', log=False)
                H = json_graph.node_link_graph(data)
                # Read a file and load it
                joined_graph = nx.compose(joined_graph, H)
        
        comp_meta = selected_meta = self.meta[(self.meta['river_km'] >= low_limit) & (self.meta['river_km'] <= up_limit)]
        comp_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        # second filter
        comp = [x for x in self.gauges if x not in comp_gauges]
        remove =[x for x in joined_graph.nodes if int(x[0]) in comp]
        print(joined_graph.nodes)
        joined_graph.remove_nodes_from(remove)
        print(joined_graph.nodes)
        
        # third filter
        remove_date = [x for x in joined_graph.nodes if ((x[1] > end_date) or (x[1] < start_date))]
        joined_graph.remove_nodes_from(remove_date)
        print(joined_graph.nodes)
        
        # fourth filter
        cc = [list(x) for x in nx.connected_components(joined_graph)]
        for sub_cc in cc:
            res_start = [int(node[0]) == start_station for node in sub_cc]
            res_end = [int(node[0]) == end_station for node in sub_cc]
            if ((True not in res_start) or (True not in res_end)):
                joined_graph.remove_nodes_from(sub_cc)
        total_waves = 0
        for sub_cc in cc:
            start_nodes = [node for node in sub_cc if int(node[0]) == start_station]
            end_nodes = [node for node in sub_cc if int(node[0]) == end_station]
            for start in start_nodes:
                for end in end_nodes:
                    paths = [list(x) for x in nx.all_shortest_paths(joined_graph, source=start, target=end)]
                    total_waves += len(paths)
        
        
        return joined_graph, total_waves
    
    def merge_graphs(self, start_station: int, end_station: int, start_date: str, end_date: str,
                    span: bool, hs: int, he:int, vs,ve):
        
        joined_graph = self.filter_graph(start_station=start_station, end_station=end_station,
                                         start_date=start_date, end_date=end_date)[0]
       
        
        
        start = datetime.strptime(start_date,'%Y-%m-%d')
        end = datetime.strptime(end_date,'%Y-%m-%d')
    
        positions = dict()
        for node in joined_graph.nodes():
            x_coord = abs((start - datetime.strptime(node[1],'%Y-%m-%d')).days) - 1
            y_coord = len(self.gauges) - self.gauges.index(int(node[0]))
            positions[node] = (x_coord, y_coord)
        
        fig, ax = plt.subplots()
        if span:
            ax.axhspan(vs, ve, color='green', alpha=0.3, label="Window for maximum")
            #ax.axvspan(hs, he, color='orange', alpha=0.3, label="Window for maximum")
        min_x = -1
        max_x = max([n[0] for n in positions.values()])
        x_labels = pd.date_range(start - timedelta(days=1),
                                 start + timedelta(days=max_x + 1),
                                 freq='d').strftime('%Y-%m-%d').tolist()
        ax.xaxis.set_ticks(np.arange(min_x - 1, max_x + 1, 1))
        ax.set_xticklabels(x_labels, rotation=20, horizontalalignment = 'right', fontsize=102)
        
        min_y = 1
        max_y = len(self.gauges) + 1
        y_labels = [str(gauge) for gauge in self.gauges[::-1]]
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(y_labels, rotation=20, horizontalalignment = 'right', fontsize=32)
        
        plt.rcParams["figure.figsize"] = (40,10)
        nx.draw(joined_graph, pos=positions, node_size=1000)
        limits=plt.axis('on') # turns on axis
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)
        plt.savefig('graph_.pdf')
        
    def plot_graph(self, start: str, end: str):
        
        if self.gauge_peak_plateau_pairs == {}:            
            self.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/step2/gauge_peak_plateau_pairs.json', log=False)
        
        self.gauge_pairs = list(self.gauge_peak_plateau_pairs.keys())

        joined_graph = nx.DiGraph()
        for gauge_pair in self.gauge_pairs:
            filenames = next(os.walk(f'./saved/step3/{gauge_pair}'), (None, None, []))[2]
            sorted_files = self.sort_wave(filenames=filenames, start=start, end=end)
            for file in sorted_files:
                data = JsonHelper.read(filepath=f'./saved/step3/{gauge_pair}/{file}', log=False)
                H = json_graph.node_link_graph(data)
                # Read a file and load it
                joined_graph = nx.compose(joined_graph, H)

        start = datetime.strptime(start,'%Y-%m-%d')
        end = datetime.strptime(end,'%Y-%m-%d')
    
        positions = dict()
        for node in joined_graph.nodes():
            x_coord = abs((start - datetime.strptime(node[1],'%Y-%m-%d')).days) - 1
            y_coord = len(self.gauges) - self.gauges.index(int(node[0]))
            positions[node] = (x_coord, y_coord)
        
        fig, ax = plt.subplots()
        ax.axhspan(4, 9, color='green', alpha=0.3, label="Window for maximum")
        min_x = -1
        max_x = max([n[0] for n in positions.values()])
       
        x_labels = pd.date_range(start - timedelta(days=1),
                                 start + timedelta(days=max_x + 1),
                                 freq='d').strftime('%Y-%m-%d').tolist()
        ax.xaxis.set_ticks(np.arange(min_x - 1, max_x + 1, 1))
        ax.set_xticklabels(x_labels, rotation=20, horizontalalignment = 'right', fontsize=15)
        
        min_y = 1
        max_y = len(self.gauges) + 1
        y_labels = [str(gauge) for gauge in self.gauges[::-1]]
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(y_labels, rotation=20, horizontalalignment = 'right', fontsize=22)
        
        plt.rcParams["figure.figsize"] = (30,20)
        nx.draw(joined_graph, pos=positions, node_size=500)
        limits=plt.axis('on') # turns on axis
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)
        plt.savefig('graph.pdf')
        
    @measure_time       
    def step_1(self):
        for gauge in self.gauges:
            if not os.path.exists('saved/step1/' + str(gauge) + '.json'):

                # Get gauge data and drop missing data and make it an array.
                gauge_df = self.dataloader.get_daily_time_series(reg_number_list=[gauge]).dropna()

                # Get local peak/plateau values
                object_array = self.create_gauge_data_2(gauge_ts=gauge_df[gauge].to_numpy())

                # Create keys for dictionary
                peak_plateau_tuples = self.create_peak_plateau_list(
                    gauge_df=gauge_df, 
                    gauge_data=object_array, 
                    reg_number=gauge
                )

                # Save
                JsonHelper.write(filepath=f'./saved/step1/{gauge}.json', obj=peak_plateau_tuples)
    
    @measure_time
    def step_2(self):
        self.search_flooding_gauge_pairs(delay=0, window_size=3, gauges=self.gauges)
    
    @measure_time
    def step_3(self):
        """
        Searching for wave "series". For now, starting from the root ('1514-1515'). 
        Trying to find the same waves in different gauges.
        """

        # Read the gauge_peak_plateau_pairs (super dict)
        if self.gauge_peak_plateau_pairs == {}:            
            self.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/step2/gauge_peak_plateau_pairs.json')
        
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

        root_gauge_pair = self.gauge_pairs[0] # Root.
        root_gauge_pair_date_dict = self.gauge_peak_plateau_pairs[root_gauge_pair]
        for gauge_pair in self.gauge_pairs:
            next_g_p_idx = 1
            root_gauge_pair_date_dict = self.gauge_peak_plateau_pairs[gauge_pair]
            os.makedirs(f'./saved/step3/{gauge_pair}', exist_ok = True)
            # Search waves starting from the root
            for actual_date in root_gauge_pair_date_dict.keys():

                self.tree_g = nx.Graph()   
                self.flood_wave = {}
                # Go over every date with a wave
                for next_date in root_gauge_pair_date_dict[actual_date]:

                    # Empty and reset variables
                    next_g_p_idx = 1
                    self.path = {}
                    self.all_paths = {}
                    self.wave_serial_number = 0
                    
                    
                    """
                    root_gauge = root_gauge_pair.split('_')[0]
                    root_gauge_next = root_gauge_pair.split('_')[1]
                    """
                    root_gauge = gauge_pair.split('_')[0]
                    root_gauge_next = gauge_pair.split('_')[1]

                    self.tree_g.add_edge(u_of_edge=(root_gauge, actual_date),
                                         v_of_edge=(root_gauge_next, next_date))
                    self.path[root_gauge] = actual_date
                    self.path[root_gauge_next] = next_date

                    # Search for flood wave
                    self.create_flood_wave(next_gauge_date=next_date,
                                      next_idx=next_g_p_idx)

                    # Go over the missed branches
                    while self.branches.qsize() != 0:

                        # Get info from branches (info about the branch)
                        new_date, new_g_p_idx, path_key = self.branches.get()
                        self.path = self.all_paths[path_key]

                        # Go back to the branch
                        self.create_flood_wave(next_gauge_date=new_date,
                                          next_idx=new_g_p_idx)

                    # Save the wave
                    
                    data = json_graph.node_link_data(self.tree_g)
                    JsonHelper.write(filepath=f'./saved/step3/{gauge_pair}/{actual_date}', obj=data)
                    
        
    @measure_time
    def run(self):
        self.mkdirs()
        self.step_1()
        self.step_2()
        self.step_3()
        