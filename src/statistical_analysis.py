import json
import os
from datetime import datetime
import networkx as nx
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.flood_wave_core.flood_wave_handler import FloodWaveHandler


class StatisticalAnalysis:

    @staticmethod
    def calculate_mean_velocity(river_kms: pd.Series, graph: nx.DiGraph) -> list:
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
        mean_velocities = []
        for i in range(1876 + length, 2020):
            args = {"start_date": f'{i - length}-01-01',
                    "end_date": f'{i}-12-31',
                    "gauge_pairs": gauge_pairs,
                    "folder_name": folder_name}
            graph = FloodWaveHandler().create_directed_graph(**args)

            velocities = StatisticalAnalysis.calculate_mean_velocity(river_kms=river_kms, graph=graph)
            mean_velocity = np.mean(velocities)

            mean_velocities.append(mean_velocity)

            StatisticalAnalysis.print_percentage(i=i, length=length)

        return mean_velocities

    @staticmethod
    def get_statistics(river_kms: pd.Series, gauges: list, gauge_pairs: list, folder_name: str) -> pd.DataFrame:
        # Create a dataframe containing some statistics
        final_table = dict()
        years = []
        components_num = []
        little = []
        big = []
        mins = []
        maxs = []
        means = []
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

            little.append(node_colors.count("yellow"))
            big.append(node_colors.count("red"))

            components = list(nx.weakly_connected_components(graph))
            components_num.append(len(components))

            velocities = StatisticalAnalysis.calculate_mean_velocity(river_kms=river_kms, graph=graph)

            mins.append(np.min(velocities))
            maxs.append(np.max(velocities))
            means.append(np.mean(velocities))

            StatisticalAnalysis.print_percentage(i=i, length=0)

        final_table["Datum (ev)"] = years
        final_table["Arhullam (db)"] = components_num
        final_table["Kisviz (db)"] = little
        final_table["Nagyviz (db)"] = big
        final_table["Min. sebesseg (km/h)"] = mins
        final_table["Max. sebesseg (km/h)"] = maxs
        final_table["Atlagsebesseg (km/h)"] = means

        return pd.DataFrame(final_table)

    @staticmethod
    def low_high_by_gauge_yearly(gauges: list, folder_name: str) -> pd.DataFrame:
        # Create a dataframe containing the number of low and high water levels
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
        x = i + 1 - (1876 + length)
        y = len(range(1876 + length, 2020))
        print(f"\r{round(100 * x / y, 1)}% done", end="")

    @staticmethod
    def get_node_colors_in_given_year(gauges: list,
                                      folder_name: str,
                                      start_date: str,
                                      end_date: str) -> dict:
        gauges_dct = {}
        for gauge in gauges:
            f = open(os.path.join(PROJECT_PATH, folder_name, "find_vertices", str(gauge) + ".json"))
            read_dct = json.load(f)

            node_colors = [read_dct[i][1] for i in list(read_dct.keys()) if start_date <= i <= end_date]
            gauges_dct[f"{gauge}"] = node_colors

        return gauges_dct
