import json
import os
from datetime import datetime
import networkx as nx
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.flood_wave_core.flood_wave_handler import FloodWaveHandler


class StatisticalAnalysis:
    """
    This class contains functions for statistically analysing flood wave graphs
    """

    @staticmethod
    def calculate_all_velocities(river_kms: pd.Series, graph: nx.DiGraph) -> list:
        """
        This function calculates the velocity of all flood waves in the input graph
        :param pd.Series river_kms: river kilometers of the gauges
        :param nx.DiGraph graph: the graph
        :return list: velocities in a list
        """
        velocities = []
        for comp in list(nx.weakly_connected_components(graph)):
            comp = list(comp)
            regs = []
            dates = []
            for node in comp:
                regs.append(float(node[0]))
                dates.append(node[1])
            regs = np.array(regs)
            date_min = min(dates)
            date_max = max(dates)
            date_min_ind = [i for i, x in enumerate(dates) if x == date_min]
            date_max_ind = [i for i, x in enumerate(dates) if x == date_max]

            regs_min = regs[date_min_ind]
            regs_max = regs[date_max_ind]

            start = max(river_kms[regs_min])
            end = min(river_kms[regs_max])

            days = (datetime.strptime(date_max, '%Y-%m-%d') - datetime.strptime(date_min, '%Y-%m-%d')).days
            distance = start - end

            if days == 0:
                velocity = distance / 24
            else:
                velocity = distance / (days * 24)

            velocities.append(velocity)

        return velocities

    @staticmethod
    def yearly_mean_moving_average(river_kms: pd.Series,
                                   gauge_pairs: list,
                                   folder_name: str,
                                   length: int) -> list:
        """
        This function calculates moving average time series of the velocities
        :param pd.Series river_kms: river kilometers of the gauges
        :param list gauge_pairs: list of the gauge pairs for creating the directed graph
        :param str folder_name: name of the generated data folder
        :param int length: length of one period in years
        :return list: moving average time series of the velocities
        """
        mean_velocities = []
        for i in range(1876 + length, 2020):
            args = {"start_date": f'{i - length}-01-01',
                    "end_date": f'{i}-12-31',
                    "gauge_pairs": gauge_pairs,
                    "folder_name": folder_name}
            graph = FloodWaveHandler().create_directed_graph(**args)

            velocities = StatisticalAnalysis.calculate_all_velocities(river_kms=river_kms, graph=graph)
            mean_velocity = np.mean(velocities)

            mean_velocities.append(mean_velocity)

            StatisticalAnalysis.print_percentage(i=i, length=length)

        return mean_velocities

    @staticmethod
    def get_statistics(river_kms: pd.Series, gauges: list, gauge_pairs: list, folder_name: str) -> pd.DataFrame:
        """
        This function creates a dataframe containing some statistics of the whole graph yearly
        :param pd.Series river_kms: river kilometers of the gauges
        :param list gauges: list of gauges
        :param list gauge_pairs: list of the gauge pairs for creating the directed graph
        :param str folder_name: name of the generated data folder
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
        for i in range(1876, 2020):
            years.append(i)
            start_date = f'{i}-01-01'
            end_date = f'{i}-12-31'
            args_create = {
                "start_date": start_date,
                "end_date": end_date,
                "gauge_pairs": gauge_pairs,
                "folder_name": folder_name
            }
            graph = FloodWaveHandler().create_directed_graph(**args_create)

            gauges_dct = StatisticalAnalysis.get_node_colors_in_given_year(gauges=gauges,
                                                                           folder_name=folder_name,
                                                                           start_date=start_date,
                                                                           end_date=end_date)

            node_colors = []
            for gauge in gauges:
                node_colors += gauges_dct[str(gauge)]

            low.append(node_colors.count("yellow"))
            high.append(node_colors.count("red"))

            components = list(nx.weakly_connected_components(graph))
            components_num.append(len(components))

            velocities = StatisticalAnalysis.calculate_all_velocities(river_kms=river_kms, graph=graph)

            mins.append(np.min(velocities))
            maxs.append(np.max(velocities))
            means.append(np.mean(velocities))
            medians.append(np.median(velocities))
            stds.append(np.std(velocities))

            StatisticalAnalysis.print_percentage(i=i, length=0)

        final_table["Datum (ev)"] = years
        final_table["Arhullam (db)"] = components_num
        final_table["Kisviz (db)"] = low
        final_table["Nagyviz (db)"] = high
        final_table["Min. sebesseg (km/h)"] = mins
        final_table["Max. sebesseg (km/h)"] = maxs
        final_table["Atlagsebesseg (km/h)"] = means
        final_table["Median sebesseg (km/h)"] = medians
        final_table["Sebessegek szorasa"] = stds

        return pd.DataFrame(final_table)

    @staticmethod
    def low_high_by_gauge_yearly(gauges: list, folder_name: str) -> pd.DataFrame:
        """
        This function creates a dataframe containing the number of low and high water level vertices by gauge yearly
        (years in the rows and gauges in the columns)
        :param list gauges: list of gauges
        :param str folder_name: name of the generated data folder
        :return pd.DataFrame: dataframe containing the number of low and high water level vertices by gauge yearly
        """
        years = []
        final_matrix = np.zeros((144, 28))
        for i in range(1876, 2020):
            years.append(i)
            start_date = f'{i}-01-01'
            end_date = f'{i}-12-31'

            gauges_dct = StatisticalAnalysis.get_node_colors_in_given_year(gauges=gauges,
                                                                           folder_name=folder_name,
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

            StatisticalAnalysis.print_percentage(i=i, length=0)

        columns = []
        for gauge in gauges:
            columns.append(f"{gauge} (low)")
            columns.append(f"{gauge} (high)")

        return pd.DataFrame(final_matrix, index=years, columns=columns)

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
    def get_node_colors_in_given_year(gauges: list,
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
            gauges_dct[f"{gauge}"] = node_colors

        return gauges_dct
