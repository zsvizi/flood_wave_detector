import json
import os

import numpy as np

from src import PROJECT_PATH


class AnalysisHandler:
    """
    This is a helper class for GraphAnalysis and StatisticalAnalysis
    """

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
    def get_start_and_end_node_of_slowest_and_longest_flood_wave(branch: list) -> tuple:
        """
        This function returns the start and end nodes of a branch. There is only one start node but there can be
        multiple end nodes. Out of the possible end nodes the one with the latest date will be returned
        :param list branch: the given branch
        :return tuple: start node and end node
        """
        regs = []
        for node in branch:
            regs.append(float(node[0]))

        max_reg = np.max(regs)
        min_reg = np.min(regs)
        max_reg_idx = regs.index(max_reg)
        min_reg_idx = [i for i, x in enumerate(regs) if x == min_reg]

        start_node = branch[max_reg_idx]

        possible_end_nodes = [branch[idx] for idx in min_reg_idx]

        possible_end_dates = []
        for pen in possible_end_nodes:
            possible_end_dates.append(pen[1])
        end_date = max(possible_end_dates)
        end_date_idx = possible_end_dates.index(end_date)

        end_node = possible_end_nodes[end_date_idx]

        return start_node, end_node
