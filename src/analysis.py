from datetime import datetime, timedelta

import networkx as nx
import numpy as np
import pandas as pd

from src.flood_wave_detector import FloodWaveDetector
from src.gauge_data import GaugeData
from src.json_helper import JsonHelper
from src.measure_time import measure_time


class Analysis:
    def __init__(self, fwd: FloodWaveDetector):
        self.fwd = fwd

    def filter_graph(
            self,
            start_station: int,
            end_station: int,
            start_date: str,
            end_date: str
    ) -> nx.Graph:

        if self.fwd.gauge_peak_plateau_pairs == {}:
            self.fwd.gauge_peak_plateau_pairs = JsonHelper.read(
                filepath='./saved/find_edges/gauge_peak_plateau_pairs.json',
                log=False
            )

        self.fwd.gauge_pairs = list(self.fwd.gauge_peak_plateau_pairs.keys())
        up_limit = self.fwd.meta.loc[start_station].river_km
        low_limit = self.fwd.meta.loc[end_station].river_km

        # first filter
        start_gauges = self.select_start_gauges(low_limit=low_limit)

        selected_pairs = [
            x
            for x in self.fwd.gauge_pairs
            if int(x.split('_')[0]) in start_gauges
        ]

        joined_graph = nx.Graph()
        for gauge_pair in selected_pairs:
            joined_graph = self.fwd.compose_graph(
                end_date=end_date,
                gauge_pair=gauge_pair,
                joined_graph=joined_graph,
                start_date=start_date
            )

        # second filter
        self.remove_nodes_with_improper_km_data(
            joined_graph=joined_graph,
            low_limit=low_limit,
            up_limit=up_limit
        )

        # third filter
        self.date_filter(
            joined_graph=joined_graph,
            start_date=start_date,
            end_date=end_date
        )

        # fourth filter
        self.remove_components_not_including_start_or_end_station(
            start_station=start_station,
            end_station=end_station,
            joined_graph=joined_graph
        )

        return joined_graph

    def select_start_gauges(
            self,
            low_limit: int
    ) -> list:

        selected_meta = self.fwd.meta[(self.fwd.meta['river_km'] >= low_limit)]
        start_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        return start_gauges

    def remove_nodes_with_improper_km_data(
            self,
            joined_graph: nx.Graph,
            low_limit: int,
            up_limit: int
    ) -> None:

        selected_meta = self.fwd.meta[(self.fwd.meta['river_km'] >= low_limit) &
                                      (self.fwd.meta['river_km'] <= up_limit)]

        comp_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        comp = [
            x
            for x in self.fwd.gauges
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
        peak_plateau_df = self.clean_dataframe_for_getting_peak_plateau_list(
            gauge_data=gauge_data,
            gauge_df=gauge_df,
            reg_number=reg_number
        )

        # Get peak-plateau list
        return self.get_peak_plateau_list(peak_plateau_df)

    @staticmethod
    def get_peak_plateau_list(peak_plateau_df: pd.DataFrame) -> list:
        peak_plateau_tuple = peak_plateau_df.to_records(index=True)
        peak_plateau_list = [
            tuple(x)
            for x in peak_plateau_tuple
        ]
        return peak_plateau_list

    @staticmethod
    def clean_dataframe_for_getting_peak_plateau_list(
            gauge_data: np.array,
            gauge_df: pd.DataFrame,
            reg_number: str
    ) -> pd.DataFrame:

        peak_plateau_df = gauge_df.loc[np.array([x.is_peak for x in gauge_data])]
        peak_plateau_df = peak_plateau_df.drop(columns="Date") \
            .set_index(peak_plateau_df.index.strftime('%Y-%m-%d'))
        peak_plateau_df[reg_number] = peak_plateau_df[reg_number].astype(float)
        return peak_plateau_df

    @staticmethod
    def convert_datetime_to_str(
            actual_date,
            actual_next_pair,
            found_next_dates
    ):

        if not found_next_dates.empty:
            found_next_dates_str = found_next_dates['Date'].dt.strftime('%Y-%m-%d').tolist()
            actual_next_pair[actual_date.strftime('%Y-%m-%d')] = found_next_dates_str

    def find_dates_for_next_gauge(
            self,
            actual_date: datetime,
            delay: int,
            next_gauge_df: pd.DataFrame,
            window_size: int
    ) -> pd.DataFrame:

        past_date = actual_date - timedelta(days=delay)
        found_next_dates = self.filter_for_start_and_length(
            gauge_df=next_gauge_df,
            min_date=past_date,
            window_size=window_size
        )
        return found_next_dates

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

    @staticmethod
    def read_data_from_gauge(gauge: str) -> pd.DataFrame:
        gauge_with_index = JsonHelper.read(f'./saved/find_vertices/{gauge}.json')
        gauge_df = pd.DataFrame(data=gauge_with_index,
                                columns=['Date', 'Max value'])
        gauge_df['Date'] = pd.to_datetime(gauge_df['Date'])
        return gauge_df

    @measure_time
    def sort_wave(
            self,
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

    @staticmethod
    def count_waves(
            joined_graph: nx.Graph,
            start_station: int,
            end_station: int
    ) -> int:

        connected_components = [
            list(x)
            for x in nx.connected_components(joined_graph)
        ]

        total_waves = 0
        for sub_connected_component in connected_components:
            start_nodes = [
                node
                for node in sub_connected_component
                if int(node[0]) == start_station
            ]
            end_nodes = [
                node
                for node in sub_connected_component
                if int(node[0]) == end_station
            ]

            for start in start_nodes:
                for end in end_nodes:
                    paths = [
                        list(x)
                        for x in nx.all_shortest_paths(joined_graph, source=start, target=end)]
                    total_waves += len(paths)
        return total_waves
