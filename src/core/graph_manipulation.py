import os
from datetime import datetime

import networkx as nx
import pickle

from src import PROJECT_PATH
from src.utils.json_helper import JsonHelper


class GraphManipulation:
    """
    This class is responsible for creating the filtered graph between the start and end dates
    """

    @staticmethod
    def create_directed_graph(
            start_date: str,
            end_date: str,
            gauge_pairs: list,
            folder_name: str
    ):
        """
        Creates a directed graph by composing directed graphs

        :param str start_date: The date of the first possible starting vertex
        :param str end_date: The date of the last possible starting vertex
        :param list gauge_pairs: The list of gauge pairs which should be included in the graph
        :param str folder_name: Name of the folder to use for file handling.
        :return nx.DiGraph: The composed directed graph
        """

        joined_graph = nx.DiGraph()
        for gauge_pair in gauge_pairs:
            print(gauge_pair)
            joined_graph = GraphManipulation.compose_graph(
                end_date=end_date,
                gauge_pair=gauge_pair,
                joined_graph=joined_graph,
                start_date=start_date,
                folder_name=folder_name
            )

        with open(f"{PROJECT_PATH}/whole_graph/joined_graph.gpickle", "wb") as f:
            pickle.dump(joined_graph, f)

    @staticmethod
    def compose_graph(
            joined_graph: nx.Graph,
            gauge_pair: str,
            start_date: str,
            end_date: str,
            folder_name: str
    ) -> nx.DiGraph:
        """
        Combines graphs that are saved out individually with one that is given into one undirected graph

        :param nx.Graph joined_graph: A graph to combine with the ones that are read from the files
        :param str gauge_pair: This gauge pair indicates the starting node of the graph
        :param str start_date: The first possible starting date for the graphs to be read
        :param str end_date: The last possible starting date for the graphs to be read
        :param str folder_name: Name of the folder to use for file handling.
        :return nx.Graph: The graph that was made by combining individually saved ones.
        """

        filenames = next(os.walk(os.path.join(PROJECT_PATH, folder_name, 'build_graph', f'{gauge_pair}')),
                         (None, None, []))[2]
        sorted_files = GraphManipulation.sort_wave(
            filenames=filenames,
            start=start_date,
            end=end_date
        )
        for file in sorted_files:
            data = JsonHelper.read(
                filepath=os.path.join(PROJECT_PATH, folder_name, 'build_graph',
                                      f'{gauge_pair}', f'{file}'),
                log=False
            )
            h = nx.readwrite.json_graph.node_link_graph(data)
            joined_graph = nx.compose(joined_graph, h)
        return joined_graph

    @staticmethod
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
