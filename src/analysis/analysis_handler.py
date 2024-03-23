import json
import os
from datetime import datetime

from src import PROJECT_PATH
from src.core.flood_wave_extractor import FloodWaveExtractor
from src.core.graph_handler import GraphHandler
from src.core.slope_calculator import SlopeCalculator
from src.selection.selection import Selection


class AnalysisHandler:
    """
    This is a helper class for GraphAnalysis and StatisticalAnalysis
    """

    @staticmethod
    def get_flood_waves_yearly(year: int, gauge_pairs: list, folder_name: str) -> list:
        """
        This function returns only those components that start in the actual year
        :param int year: the actual year
        :param list gauge_pairs: list of gauge pairs
        :param str folder_name: the name of the generated data folder
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
        args = {"start_date": start_date,
                "end_date": end_date,
                "gauge_pairs": gauge_pairs,
                "folder_name": folder_name}
        graph = GraphHandler.create_directed_graph(**args)

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

    @staticmethod
    def get_node_colors_in_given_period(gauges: list,
                                        folder_name: str,
                                        start_date: str,
                                        end_date: str) -> dict:
        """
        This function creates a dictionary with "gauge": colors type key-value pairs where colors is a list containing
        the colors of the vertices corresponding to gauge
        :param list gauges: list of gauges
        :param str folder_name: name of the generated data folder
        :param str start_date: start date as a string
        :param str end_date: end date as a string
        :return dict: the dictionary described above
        """
        gauges_dct = {}
        for gauge in gauges:
            f = open(os.path.join(PROJECT_PATH, folder_name, "find_vertices", str(gauge) + ".json"))
            read_dct = json.load(f)

            node_colors = [read_dct[i][1] for i in list(read_dct.keys()) if start_date <= i <= end_date]
            gauges_dct[str(gauge)] = node_colors

        return gauges_dct

    @staticmethod
    def get_slopes_list(vertex_pairs: dict, vtx_pair: str, valid_dates: list) -> list:
        valid_slopes = [vertex_pairs[vtx_pair][valid_date][1] for valid_date in valid_dates]
        flattened_slopes = [item for sublist in valid_slopes for item in
                            (sublist if isinstance(sublist, list) else [sublist])]

        return flattened_slopes

    @staticmethod
    def get_slopes_interval_list(start_station: str,
                                 end_station: str,
                                 start_date: str,
                                 end_date: str,
                                 gauge_pairs: list,
                                 sorted_stations: list,
                                 folder_name: str):
        args_create = {
            "start_date": start_date,
            "end_date": end_date,
            "gauge_pairs": gauge_pairs,
            "folder_name": folder_name
        }
        graph = GraphHandler.create_directed_graph(**args_create)

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
                                         folder_name=folder_name)

            current_date = datetime.strptime(start_node[1], "%Y-%m-%d")
            slope = slope_calc.get_slopes(current_date=current_date, next_dates=[end_node[1]])

            slopes.append(slope[0])

        return slopes
