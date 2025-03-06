import json
import os
import pickle
from datetime import datetime

import networkx as nx
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.analysis.analysis_handler import AnalysisHandler
from src.core.flood_wave_extractor import FloodWaveExtractor
from src.data.dataloader import Dataloader
from src.selection.selection import Selection


class StatisticalAnalysis:
    """
    This class contains functions for statistically analysing flood waves
    """
    def __init__(self, folder_name: str):
        self.folder_name = folder_name
        with open(os.path.join(
                PROJECT_PATH, 'whole_graph', 'joined_graph.gpickle'
        ), 'rb') as f:
            self.graph_whole = pickle.load(f)

    def yearly_mean_moving_average(self, length: int) -> list:
        """
        This function calculates moving average time series of the velocities
        :param int length: length of one period in years
        :return list: moving average time series of the velocities
        """
        mean_velocities = []
        for i in range(1876 + length, 2020):
            start_date = f'{i - length}-01-01'
            end_date = f'{i}-12-31'
            graph = \
                Selection.select_time_interval(joined_graph=self.graph_whole, start_date=start_date, end_date=end_date)

            velocities = StatisticalAnalysis.calculate_all_velocities(joined_graph=graph)
            mean_velocity = np.mean(velocities)

            mean_velocities.append(mean_velocity)

            AnalysisHandler.print_percentage(i=i, length=length)

        return mean_velocities

    def get_statistics(self, gauges: list) -> pd.DataFrame:
        """
        This function creates a dataframe containing some statistics of the whole graph yearly
        :param list gauges: list of gauges
        :return pd.DataFrame: dataframe of the following statistics yearly: number of components,
        number of low and high water level vertices, minimum and maximum velocities, average of velocities
        """
        final_table = dict()
        years = []
        components_num = []
        low = []
        high = []
        mins = []
        maxs = []
        means = []
        medians = []
        stds = []
        a_handler = AnalysisHandler(graph_whole=self.graph_whole, folder_name=self.folder_name)
        for i in range(1876, 2020):
            years.append(i)
            start_date = f'{i}-01-01'
            end_date = f'{i}-12-31'
            graph = \
                Selection.select_time_interval(joined_graph=self.graph_whole, start_date=start_date, end_date=end_date)

            gauges_dct = a_handler.get_node_colors_in_given_period(gauges=gauges,
                                                                   start_date=start_date,
                                                                   end_date=end_date)

            node_colors = []
            for gauge in gauges:
                node_colors += gauges_dct[str(gauge)]

            low.append(node_colors.count("yellow"))
            high.append(node_colors.count("red"))

            extractor = FloodWaveExtractor(joined_graph=graph)
            extractor.get_flood_waves()
            flood_waves = extractor.flood_waves
            components_num.append(len(flood_waves))

            velocities = StatisticalAnalysis.calculate_all_velocities(joined_graph=graph)

            mins.append(np.min(velocities))
            maxs.append(np.max(velocities))
            means.append(np.mean(velocities))
            medians.append(np.median(velocities))
            stds.append(np.std(velocities))

            AnalysisHandler.print_percentage(i=i, length=0)

        final_table["Datum (ev)"] = years
        final_table["Arhullam (db)"] = components_num
        final_table["Kisviz (db)"] = low
        final_table["Nagyviz (db)"] = high
        final_table["Min. sebesseg (km/day)"] = mins
        final_table["Max. sebesseg (km/day)"] = maxs
        final_table["Atlagsebesseg (km/day)"] = means
        final_table["Median sebesseg (km/day)"] = medians
        final_table["Sebessegek szorasa"] = stds

        return pd.DataFrame(final_table)

    def low_high_by_gauge_yearly(self, gauges: list) -> pd.DataFrame:
        """
        This function creates a dataframe containing the number of low and high water level vertices by gauge yearly
        (years in the rows and gauges in the columns)
        :param list gauges: list of gauges
        :return pd.DataFrame: dataframe containing the number of low and high water level vertices by gauge yearly
        """
        years = []
        final_matrix = np.zeros((144, 28))
        a_handler = AnalysisHandler(graph_whole=self.graph_whole, folder_name=self.folder_name)
        for i in range(1876, 2020):
            years.append(i)
            start_date = f'{i}-01-01'
            end_date = f'{i}-12-31'

            gauges_dct = a_handler.get_node_colors_in_given_period(gauges=gauges,
                                                                   start_date=start_date,
                                                                   end_date=end_date)

            k = 1
            for gauge in gauges:
                gauge_colors = gauges_dct[str(gauge)]
                k_yellow = gauge_colors.count("yellow")
                k_red = gauge_colors.count("red")
                final_matrix[i - 1876, 2 * k - 2] = k_yellow
                final_matrix[i - 1876, 2 * k - 1] = k_red
                k += 1

            AnalysisHandler.print_percentage(i=i, length=0)

        columns = []
        for gauge in gauges:
            columns.append(f"{gauge} (low)")
            columns.append(f"{gauge} (high)")

        return pd.DataFrame(final_matrix, index=years, columns=columns)

    def analyse_slopes_by_vertex_pairs(self, period: int):
        """
        This method goes through the vertex pairs and calculates some {period}-year statistics from 1876 to 2019
        concerning the slopes on the edges between the given vertex pair. It then saves the dataframes into one
        table with the following statistics: minimums, maximums, means, medians and standard deviations
        :param int period: the results are accumulated for this many years
        """
        f = open(os.path.join(PROJECT_PATH, self.folder_name, "find_edges", "vertex_pairs.json"))
        vertex_pairs = json.load(f)

        years = np.arange(1876, 2020, period)
        dfs = []
        for vtx_pair in list(vertex_pairs.keys()):
            final_table = {}
            mins = []
            maxs = []
            means = []
            medians = []
            stds = []
            indices = []
            for year in years:
                if year + period - 1 > 2019:
                    break

                start_date = f'{year}-01-01'
                end_date = f'{year + period - 1}-12-31'
                current_dates = list(vertex_pairs[vtx_pair].keys())

                if all(j < start_date or j > end_date for j in current_dates):
                    continue
                else:
                    indices.append(f'{start_date}_{end_date}')
                    valid_dates = [x for x in current_dates if start_date <= x <= end_date]

                flattened_slopes = AnalysisHandler.get_slopes_list(vertex_pairs=vertex_pairs,
                                                                   vtx_pair=vtx_pair,
                                                                   valid_dates=valid_dates)

                mins.append(np.min(flattened_slopes))
                maxs.append(np.max(flattened_slopes))
                means.append(np.mean(flattened_slopes))
                medians.append(np.median(flattened_slopes))
                stds.append(np.std(flattened_slopes))

            final_table["Min. slope (cm/km)"] = mins
            final_table["Max. slope (cm/km)"] = maxs
            final_table["Mean"] = means
            final_table["Median"] = medians
            final_table["Standard deviation"] = stds

            df = pd.DataFrame(final_table, index=indices)

            dfs.append(df)

        with pd.ExcelWriter(f'{period}-year_slopes_by_vertex_pairs.xlsx') as writer:
            for i, df in enumerate(dfs):
                sheet_name = f'{list(vertex_pairs.keys())[i]}'
                df.to_excel(writer, sheet_name=sheet_name)

    def analyse_slopes_in_interval(self,
                                   start_station: str,
                                   end_station: str,
                                   period: int,
                                   sorted_stations: list) -> pd.DataFrame:
        """
        This function calculates statistics of slopes of paths in flood waves
        :param str start_station: string of starting station number
        :param str end_station: string of ending station number
        :param int period: the results are accumulated for this many years
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return pd.DataFrame: dataframe containing the statistics
        """
        final_table = {}
        indices = []
        mins = []
        maxs = []
        means = []
        medians = []
        stds = []

        years = np.arange(1876, 2020, period)
        a_handler = AnalysisHandler(graph_whole=self.graph_whole, folder_name=self.folder_name)
        for year in years:
            if year + period - 1 > 2019:
                break

            start_date = f'{year}-01-01'
            end_date = f'{year + period - 1}-12-31'

            indices.append(start_date + '_' + end_date)

            slopes = a_handler.get_slopes_interval_list(start_station=start_station,
                                                        end_station=end_station,
                                                        start_date=start_date,
                                                        end_date=end_date,
                                                        sorted_stations=sorted_stations)

            mins.append(np.min(slopes))
            maxs.append(np.max(slopes))
            means.append(np.mean(slopes))
            medians.append(np.median(slopes))
            stds.append(np.std(slopes))

        final_table["Min. slope (cm/km)"] = mins
        final_table["Max. slope (cm/km)"] = maxs
        final_table["Mean"] = means
        final_table["Median"] = medians
        final_table["Standard deviation"] = stds

        return pd.DataFrame(final_table, index=indices)

    def collect_slopes(self,
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
        pair = str(start_station) + '_' + str(end_station)

        f = open(os.path.join(PROJECT_PATH, self.folder_name, "find_edges", "vertex_pairs.json"))
        vertex_pairs = json.load(f)

        if pair in list(vertex_pairs.keys()):
            current_dates = list(vertex_pairs[pair].keys())
            valid_dates = [x for x in current_dates if start_date <= x <= end_date]
            slopes = AnalysisHandler.get_slopes_list(vertex_pairs=vertex_pairs,
                                                     vtx_pair=pair,
                                                     valid_dates=valid_dates)
        else:
            a_handler = AnalysisHandler(graph_whole=self.graph_whole, folder_name=self.folder_name)
            slopes = a_handler.get_slopes_interval_list(start_station=start_station,
                                                        end_station=end_station,
                                                        start_date=start_date,
                                                        end_date=end_date,
                                                        sorted_stations=sorted_stations)

        return slopes

    def red_ratio(self, gauges: list, period: int) -> pd.DataFrame:
        """
        This function calculates the ratio of high water level nodes and all nodes in every {period}-year period
        from 1876 to 2019
        :param list gauges: list of gauges
        :param int period: the results are accumulated for this many years
        :return pd.DataFrame: dataframe containing the ratios
        """
        indices = []
        ratios = []
        final_table = {}
        years = np.arange(1876, 2020, period)
        a_handler = AnalysisHandler(graph_whole=self.graph_whole, folder_name=self.folder_name)
        for year in years:
            if year + period - 1 > 2019:
                break

            start_date = f'{year}-01-01'
            end_date = f'{year + period - 1}-12-31'

            indices.append(f'{start_date}_{end_date}')

            gauges_dct = a_handler.get_node_colors_in_given_period(gauges=gauges,
                                                                   start_date=start_date,
                                                                   end_date=end_date)

            all_colors = []
            for gauge in gauges:
                all_colors += gauges_dct[str(gauge)]

            reds = all_colors.count("red")

            ratios.append(reds/len(all_colors))

        final_table["ratio"] = ratios

        return pd.DataFrame(final_table, index=indices)

    def get_number_of_flood_waves_yearly(self) -> list:
        """
        This function calculates the number of cleaned flood waves yearly
        :return list: numbers of cleaned components
        """
        number_of_flood_waves = []
        a_handler = AnalysisHandler(graph_whole=self.graph_whole, folder_name=self.folder_name)
        for i in range(1876, 2020):
            waves = a_handler.get_flood_waves_yearly(year=i)
            number_of_flood_waves.append(len(waves))

            AnalysisHandler.print_percentage(i=i, length=0)

        return number_of_flood_waves

    @staticmethod
    def calculate_all_velocities(joined_graph: nx.DiGraph) -> list:
        """
        This function calculates the velocity of all flood waves in the input graph
        :param nx.DiGraph joined_graph: the graph
        :return list: velocities in a list
        """
        meta = Dataloader.get_metadata()
        river_kms = meta["river_km"]

        extractor = FloodWaveExtractor(joined_graph=joined_graph)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        velocities = []
        for wave in flood_waves:
            start_node = wave[0]
            end_node = wave[1]

            start = river_kms[float(start_node[0])]
            end = river_kms[float(end_node[0])]
            distance = start - end

            days = (datetime.strptime(end_node[1], '%Y-%m-%d') - datetime.strptime(start_node[1], '%Y-%m-%d')).days

            if days == 0:
                velocity = distance
            else:
                velocity = distance / days

            velocities.append(velocity)

        return velocities
