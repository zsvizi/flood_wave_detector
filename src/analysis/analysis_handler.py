import json
import os
from datetime import datetime

import networkx as nx

from src import PROJECT_PATH
from src.core.flood_wave_extractor import FloodWaveExtractor
from src.core.slope_calculator import SlopeCalculator
from src.selection.selection import Selection


class AnalysisHandler:
    """
    This is a helper class for GraphAnalysis and StatisticalAnalysis
    """
    def __init__(self, graph_whole: None, folder_name: str):
        self.folder_name = folder_name
        if graph_whole is None:
            self.graph_whole = nx.read_gpickle(f"../whole_graph/joined_graph.gpickle")
        else:
            self.graph_whole = graph_whole

    def get_flood_waves_yearly(self, year: int) -> list:
        """
        This function returns only those components that start in the actual year
        :param int year: the actual year
        :return list: cleaned components
        """
        if year == 1876:
            start_date = f'{year}-01-01'
            end_date = f'{year + 1}-02-01'
        elif year == 2019:
            start_date = f'{year - 1}-11-30'
            end_date = f'{year}-12-31'
        else:
            start_date = f'{year - 1}-11-30'
            end_date = f'{year + 1}-02-01'
        graph = Selection.select_time_interval(joined_graph=self.graph_whole, start_date=start_date, end_date=end_date)

        extractor = FloodWaveExtractor(joined_graph=graph)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        cleaned_waves = []
        for wave in flood_waves:
            node_dates = [node[1] for node in wave]
            if not any(str(year - 1) in node_date for node_date in node_dates) \
                    and not all(str(year + 1) in node_date for node_date in node_dates):
                cleaned_waves.append(wave)

        return cleaned_waves

    @staticmethod
    def print_percentage(i: int, length: int) -> None:
        """
        This method displays what percentage of the loop is already done
        :param int i: loop counter
        :param int length: length parameter of yearly_mean_moving_average()
        """
        x = i + 1 - (1876 + length)
        y = len(range(1876 + length, 2020))
        print(f"\r{round(100 * x / y, 1)}% done", end="")

    def get_node_colors_in_given_period(self,
                                        gauges: list,
                                        start_date: str,
                                        end_date: str) -> dict:
        """
        This function creates a dictionary with "gauge": colors type key-value pairs where colors is a list containing
        the colors of the vertices corresponding to gauge
        :param list gauges: list of gauges
        :param str start_date: start date as a string
        :param str end_date: end date as a string
        :return dict: the dictionary described above
        """
        gauges_dct = {}
        for gauge in gauges:
            f = open(os.path.join(PROJECT_PATH, self.folder_name, "find_vertices", str(gauge) + ".json"))
            read_dct = json.load(f)

            node_colors = [read_dct[i][1] for i in list(read_dct.keys()) if start_date <= i <= end_date]
            gauges_dct[str(gauge)] = node_colors

        return gauges_dct

    @staticmethod
    def get_slopes_list(vertex_pairs: dict, vtx_pair: str, valid_dates: list) -> list:
        """
        This function collects the slopes on edges between adjacent stations (vtx_pair)
        :param dict vertex_pairs: dictionary of vertex pairs
        :param str vtx_pair: given vertex pair
        :param list valid_dates: dates of nodes at the starting station
        :return list: slopes
        """
        valid_slopes = [vertex_pairs[vtx_pair][valid_date][1] for valid_date in valid_dates]
        flattened_slopes = [item for sublist in valid_slopes for item in
                            (sublist if isinstance(sublist, list) else [sublist])]

        return flattened_slopes

    def get_slopes_interval_list(self,
                                 start_station: str,
                                 end_station: str,
                                 start_date: str,
                                 end_date: str,
                                 sorted_stations: list) -> list:
        """
        This function collects the slopes on edges between start_station and end_station (space), and start_date and
        end_date (time)
        :param str start_station: starting station
        :param str end_station: end station
        :param str start_date: starting date
        :param str end_date: end date
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return list: slopes
        """
        graph = Selection.select_time_interval(joined_graph=self.graph_whole, start_date=start_date, end_date=end_date)

        select_all_in_interval = Selection.select_only_in_interval(joined_graph=graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station,
                                                                   sorted_stations=sorted_stations)

        extractor = FloodWaveExtractor(joined_graph=select_all_in_interval)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        full_waves = FloodWaveExtractor.get_flood_waves_from_start_to_end(waves=flood_waves,
                                                                          start_station=start_station,
                                                                          end_station=end_station,
                                                                          equivalence=True)
        slopes = []
        for wave in full_waves:
            start_node = wave[0]
            end_node = wave[1]

            slope_calc = SlopeCalculator(current_gauge=start_node[0],
                                         next_gauge=end_node[0],
                                         folder_name=self.folder_name)

            current_date = datetime.strptime(start_node[1], "%Y-%m-%d")
            slope = slope_calc.get_slopes(current_date=current_date, next_dates=[end_node[1]])

            slopes.append(slope[0])

        return slopes
