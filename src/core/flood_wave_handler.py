import itertools

import networkx as nx

from src.selection.selection import Selection


class FloodWaveHandler:

    @staticmethod
    def get_final_pairs(joined_graph: nx.DiGraph, comp: list) -> list:
        """
        Searches for end nodes of flood waves in a connected component
        :param nx.DiGraph joined_graph: the graph
        :param list comp: the component
        :return list: list of start and end nodes of flood waves
        """
        possible_start_nodes = []
        possible_end_nodes = []
        for node in comp:
            in_deg = joined_graph.in_degree(node)
            out_deg = joined_graph.out_degree(node)

            if in_deg == 0:
                possible_start_nodes.append(node)
            if out_deg == 0:
                possible_end_nodes.append(node)

        cartesian_pairs = list(itertools.product(possible_start_nodes, possible_end_nodes))

        final_pairs = [(x, y) for x, y in cartesian_pairs if float(x[0]) > float(y[0])]

        return final_pairs

    @staticmethod
    def select_components_from_start_to_end(joined_graph: nx.DiGraph,
                                            start_station: str,
                                            end_station: str,
                                            sorted_stations: list) -> nx.DiGraph:
        """
        This function selects those components that have nodes at both start_station and end_station
        :param nx.DiGraph joined_graph: the graph
        :param str start_station: the first station in the interval
        :param str end_station: the last station in the interval
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return nx.DiGraph: the selected graph
        """
        select_all_in_interval = Selection.select_only_in_interval(joined_graph=joined_graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station,
                                                                   sorted_stations=sorted_stations)

        components = nx.weakly_connected_components(select_all_in_interval)

        selected = []
        for comp in components:
            stations = []
            for node in comp:
                stations.append(node[0])

            if any(start_station == elem for elem in stations) and any(end_station == elem for elem in stations):
                selected.append(comp)

        nodes = []
        for comp in selected:
            nodes += comp

        selected_graph = joined_graph.subgraph(nodes=nodes)

        return selected_graph
