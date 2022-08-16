from datetime import datetime, timedelta
import os

import networkx as nx
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.json_helper import JsonHelper
from src.measure_time import measure_time


class FloodWaveHandler:
    """This is a helper class for FloodWaveDetector.

    It contains functions which shrink the FloodWaveDetector class.
    e.g.: file reading, file sorting, date conversion, graph filtering, etc...
    """

    @staticmethod
    def read_vertex_file(gauge: str) -> pd.DataFrame:
        """
        Reads the generated vertex file of the station with the given ID

        :param str gauge: the ID of the desired station
        :return pd.DataFrame: A Dataframe with the peak value and date
        """
        gauge_with_index = JsonHelper.read(os.path.join(PROJECT_PATH, 'generated', 'find_vertices', f'{gauge}.json'))
        gauge_peaks = pd.DataFrame(data=gauge_with_index,
                                   columns=['Date', 'Max value'])
        gauge_peaks['Date'] = pd.to_datetime(gauge_peaks['Date'])
        return gauge_peaks

    @staticmethod
    def find_dates_for_next_gauge(
            actual_date: datetime,
            delay: int,
            next_gauge_candidate_vertices: pd.DataFrame,
            window_size: int
    ) -> pd.DataFrame:
        """
        Searches for continuation of a flood wave.

        :param datetime actual_date: The date of the last peak
        :param int delay: The allowed delay backwards for the next peak to be considered continuing
        :param pd.DataFrame next_gauge_candidate_vertices: The time series of the subsequent station in a DataFrame
        :param int window_size: The allowed window size in which we consider continuation
        :return pd.DataFrame: A DataFrame containing the found dates
        """

        past_date = actual_date - timedelta(days=delay)
        dates = FloodWaveHandler.filter_for_start_and_length(
            candidate_vertices=next_gauge_candidate_vertices,
            min_date=past_date,
            window_size=window_size
        )
        return dates

    @staticmethod
    def convert_datetime_to_str(
            actual_date: datetime,
            gauge_pair: dict,
            next_gauge_dates: pd.DataFrame
    ) -> None:
        """
        Converts the date(s) to our desired format string. Then the list of converted strings is stored in a dictionary

        :param datetime actual_date: The date to be converted
        :param dict gauge_pair: A dictionary to store the converted list of strings
        :param pd.DataFrame next_gauge_dates: A DataFrame containing the found dates to be converted
        :return:
        """

        if not next_gauge_dates.empty:
            found_next_dates_str = next_gauge_dates['Date'].dt.strftime('%Y-%m-%d').tolist()
            gauge_pair[actual_date.strftime('%Y-%m-%d')] = found_next_dates_str

    @staticmethod
    @measure_time
    def sort_wave(
                  filenames: list,
                  start: str = '2006-02-01',
                  end: str = '2006-06-01'
                  ) -> list:
        """
        It's hard to visualize waves far from each other.
        With this method, we can choose a period and check the waves in it.

        :param list filenames: List of filenames we want to choose from. (Usually all files from the directory)
        :param str start: Start date of the interval.
        :param str end: Final day of the interval.
        :return list filename_sort: List of filenames with waves in the given interval.
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

    @staticmethod
    def filter_graph(
                     start_station: int,
                     end_station: int,
                     start_date: str,
                     end_date: str,
                     gauges: list,
                     meta: pd.DataFrame,
                     ) -> nx.Graph:
        """
        Filters out the full composed graph. The parts in between the desired station and in the desired time
        interval stays if they contain a shortest path between the endpoints.

        :param int start_station: The ID of the desired starting station
        :param int end_station: The ID of the desired end station
        :param str start_date: The first possible starting date for the node to be kept
        :param str end_date: The last possible starting date for the node to be kept
        :param list gauges: The list of stations
        :param pd.DataFrame meta: A metadata table
        :return nx.Graph: The filtered graph
        """

        vertex_pairs = JsonHelper.read(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', 'vertex_pairs.json'),
                log=False
            )

        gauge_pairs = list(vertex_pairs.keys())
        up_limit = meta.loc[start_station].river_km
        low_limit = meta.loc[end_station].river_km

        # first filter
        start_gauges = FloodWaveHandler.select_start_gauges(low_limit=low_limit, meta=meta)

        selected_pairs = [
            x
            for x in gauge_pairs
            if int(x.split('_')[0]) in start_gauges
        ]

        joined_graph = nx.Graph()
        for gauge_pair in selected_pairs:
            joined_graph = FloodWaveHandler.compose_graph(
                end_date=end_date,
                gauge_pair=gauge_pair,
                joined_graph=joined_graph,
                start_date=start_date
            )

        # second filter
        FloodWaveHandler.remove_nodes_with_improper_km_data(
            joined_graph=joined_graph,
            low_limit=low_limit,
            up_limit=up_limit,
            gauges=gauges,
            meta=meta
        )

        # third filter
        FloodWaveHandler.date_filter(
            joined_graph=joined_graph,
            start_date=start_date,
            end_date=end_date
        )

        # fourth filter
        FloodWaveHandler.remove_components_not_including_start_or_end_station(
            start_station=start_station,
            end_station=end_station,
            joined_graph=joined_graph
        )

        return joined_graph

    @staticmethod
    def filter_graph_2(
                     start_station: int,
                     end_station: int,
                     start_date: str,
                     end_date: str,
                     gauges: list,
                     meta: pd.DataFrame,
                     ) -> nx.Graph:
        """
        Filters out the full composed graph. The parts in between the desired station and in the desired time
        interval stays if they contain a shortest path between the endpoints.

        :param int start_station: The ID of the desired starting station
        :param int end_station: The ID of the desired end station
        :param str start_date: The first possible starting date for the node to be kept
        :param str end_date: The last possible starting date for the node to be kept
        :param list gauges: The list of stations
        :param pd.DataFrame meta: A metadata table
        :return nx.Graph: The filtered graph
        """

        vertex_pairs = JsonHelper.read(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', 'vertex_pairs.json'),
                log=False
            )

        gauge_pairs = list(vertex_pairs.keys())
        up_limit = meta.loc[start_station].river_km
        low_limit = meta.loc[end_station].river_km

        # first filter
        start_gauges = FloodWaveHandler.select_start_gauges(low_limit=low_limit, meta=meta)

        selected_pairs = [
            x
            for x in gauge_pairs
            if int(x.split('_')[0]) in start_gauges
        ]

        joined_graph = nx.Graph()
        for gauge_pair in selected_pairs:
            joined_graph = FloodWaveHandler.compose_graph(
                end_date=end_date,
                gauge_pair=gauge_pair,
                joined_graph=joined_graph,
                start_date=start_date
            )

        # second filter
        FloodWaveHandler.remove_nodes_with_improper_km_data(
            joined_graph=joined_graph,
            low_limit=low_limit,
            up_limit=up_limit,
            gauges=gauges,
            meta=meta
        )

        # third filter
        FloodWaveHandler.date_filter(
            joined_graph=joined_graph,
            start_date=start_date,
            end_date=end_date
        )

        return joined_graph

    @staticmethod
    def select_start_gauges(
                            low_limit: int,
                            meta: pd.DataFrame
                            ) -> list:
        """
        Selects the possible starting stations for the desired flood waves, the rest is not kept.

        :param int low_limit: The river kilometre limit which tells where the lowest possible starting station should be
        :param pd.DataFrame meta: A metadata table
        :return list: List of possible starting stations
        """

        selected_meta = meta[(meta['river_km'] >= low_limit)]
        start_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        return start_gauges

    @staticmethod
    def remove_nodes_with_improper_km_data(
                                           joined_graph: nx.Graph,
                                           low_limit: int,
                                           up_limit: int,
                                           meta: pd.DataFrame,
                                           gauges: list
                                           ) -> None:
        """
        Filters out vertices which are outside the river kilometre boundaries from a given graph

        :param nx.Graph joined_graph: A graph to filter out
        :param int low_limit: The lower river kilometre limit
        :param int up_limit: The upper river kilometre limit
        :param pd.DataFrame meta: A metadata table
        :param list gauges: List of stations to remove from
        :return:
        """

        selected_meta = meta[(meta['river_km'] >= low_limit) &
                             (meta['river_km'] <= up_limit)]

        comp_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        comp = [
            x
            for x in gauges
            if x not in comp_gauges
        ]
        remove = [
            x
            for x in joined_graph.nodes
            if int(x[0]) in comp
        ]
        joined_graph.remove_nodes_from(remove)

    @staticmethod
    def date_filter(
            joined_graph: nx.Graph,
            end_date: str,
            start_date: str
    ) -> None:
        """
        Filters out all the vertices with dates outside the desired interval from a given graph.

        :param nx.Graph joined_graph: A graph to filter out
        :param str end_date: The last possible starting date for the node to be kept
        :param str start_date: The first possible starting date for the node to be kept
        :return:
        """

        remove_date = [
            x
            for x in joined_graph.nodes
            if ((x[1] > end_date) or (x[1] < start_date))
        ]
        joined_graph.remove_nodes_from(remove_date)

    @staticmethod
    def remove_components_not_including_start_or_end_station(
            start_station: int,
            end_station: int,
            joined_graph: nx.Graph
    ) -> None:
        """
        Filters out all the connected components from a graph which aren't contain a shortest path from the starting
        station to the end station.

        :param int start_station: The ID of the desired station as a starting point
        :param int end_station: The ID of the desired station as ending point
        :param nx.Graph joined_graph: A graph to filter out
        :return:
        """

        connected_components = [
            list(x)
            for x in nx.connected_components(joined_graph)
        ]

        for sub_connected_component in connected_components:
            res_start = [
                int(node[0]) == start_station
                for node in sub_connected_component
            ]
            res_end = [
                int(node[0]) == end_station
                for node in sub_connected_component
            ]
            if (True not in res_start) or (True not in res_end):
                joined_graph.remove_nodes_from(sub_connected_component)

    @staticmethod
    def get_peak_list(peaks: pd.DataFrame) -> list:
        """
        Creates a list containing (date, value) tuples.
        :param pd.DataFrame peaks: single column DataFrame which to convert
        :return list: Tuple list
        """
        # TODO: Renaming suggestion: get_peak_tuples / get_peak_list
        peak_tuples = peaks.to_records(index=True)
        peak_list = [
            tuple(x)
            for x in peak_tuples
        ]
        return peak_list

    @staticmethod
    def clean_dataframe_for_getting_peak_list(
            local_peak_values: np.array,
            gauge_data: pd.DataFrame,
            reg_number: str
    ) -> pd.DataFrame:
        """
        Creates a dataframe containing a given station's peaks with the desired date format and data type

        :param np.array local_peak_values: The flagged time series of the desired station in a numpy array
        :param pd.DataFrame gauge_data: The time series of the desired station in a DataFrame
        :param str reg_number: The ID of the desired station
        :return pd.DataFrame: A DataFrame containing the given station's peaks with date index
        """

        peaks = gauge_data.loc[np.array([x.is_peak for x in local_peak_values])]
        peaks = peaks.drop(columns="Date") \
            .set_index(peaks.index.strftime('%Y-%m-%d'))
        peaks[reg_number] = peaks[reg_number].astype(float)
        return peaks

    @staticmethod
    def filter_for_start_and_length(
            candidate_vertices: pd.DataFrame,
            min_date: datetime,
            window_size: int
    ) -> pd.DataFrame:
        """
        Find possible follow-up dates for the flood wave coming from the previous gauge

        :param pd.DataFrame candidate_vertices: Dataframe to crop
        :param datetime min_date: start date of the crop
        :param int window_size: size of the new dataframe (number of days we want)
        :return pd.DataFrame: Cropped dataframe with found next dates.
        """

        max_date = min_date + timedelta(days=window_size)
        possible_dates = candidate_vertices[(candidate_vertices['Date'] >= min_date) &
                                            (candidate_vertices['Date'] <= max_date)]

        return possible_dates

    @staticmethod
    def compose_graph(
                      joined_graph: nx.Graph,
                      gauge_pair: str,
                      start_date: str,
                      end_date: str
                      ) -> nx.Graph:
        """
        Combines graphs that are saved out individually with one that is given into one undirected graph

        :param nx.Graph joined_graph: A graph to combine with the ones that are read from the files
        :param str gauge_pair: This gauge pair indicates the starting node of the graph
        :param str start_date: The first possible starting date for the graphs to be read
        :param str end_date: The last possible starting date for the graphs to be read
        :return nx.Graph:
        """

        filenames = next(os.walk(os.path.join(PROJECT_PATH, 'generated', 'build_graph', f'{gauge_pair}')),
                         (None, None, []))[2]
        sorted_files = FloodWaveHandler.sort_wave(
            filenames=filenames,
            start=start_date,
            end=end_date
        )
        for file in sorted_files:
            data = JsonHelper.read(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'build_graph', f'{gauge_pair}', f'{file}'),
                log=False
            )
            h = nx.readwrite.json_graph.node_link_graph(data)
            joined_graph = nx.compose(joined_graph, h)
        return joined_graph

    @staticmethod
    def create_positions(
                         joined_graph: nx.Graph,
                         start: datetime.strptime,
                         gauges: list
                         ) -> dict:
        """
        Creates coordinates for a given graph in order to be able to plot it on a grid

        :param nx.Graph joined_graph: The graph which to give coordinates for
        :param datetime.strptime start: Starting date of the plot
        :param list gauges: The list of stations
        :return dict: A dictionary containing 'node: (x, y)' pairs
        """

        positions = dict()
        for node in joined_graph.nodes():
            x_coord = abs((start - datetime.strptime(node[1], '%Y-%m-%d')).days) - 1
            y_coord = len(gauges) - gauges.index(int(node[0]))
            positions[node] = (x_coord, y_coord)
        return positions

    @staticmethod
    def create_directed_graph(
                              start_date: str,
                              end_date: str,
                              gauge_pairs: list,
                              ) -> nx.DiGraph:
        """
        Creates a directed graph by composing directed graphs

        :param str start_date: The date of the first possible starting vertex
        :param str end_date: The date of the last possible starting vertex
        :param list gauge_pairs: The list of gauge pairs which should be included in the graph
        :return nx.DiGraph:
        """

        joined_graph = nx.DiGraph()
        for gauge_pair in gauge_pairs:
            joined_graph = FloodWaveHandler.compose_graph(
                end_date=end_date,
                gauge_pair=gauge_pair,
                joined_graph=joined_graph,
                start_date=start_date
            )
        return joined_graph
