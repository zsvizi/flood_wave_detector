from collections import defaultdict
from datetime import datetime
from typing import Callable

import networkx as nx
import numpy as np
import pandas as pd

from src.analysis.analysis_handler import AnalysisHandler
from src.selection.selection import Selection


class GraphAnalysis:
    """This is an analysis class for flood waves.

    Any method that does calculation or information extraction on the already existing flood wave graph structure
    belongs here.
    """

    @staticmethod
    def get_flood_waves(joined_graph: nx.DiGraph) -> list:
        """
        This function returns the actual flood waves in the graph
        :param nx.DiGraph joined_graph: the graph
        :return list: list of lists of the flood wave nodes
        """
        components = list(nx.weakly_connected_components(joined_graph))

        waves = []
        for comp in components:
            final_pairs = AnalysisHandler.get_final_pairs(joined_graph=joined_graph, comp=list(comp))

            for start, end in final_pairs:
                try:
                    wave = nx.shortest_path(joined_graph, start, end)
                    waves.append(wave)
                except nx.NetworkXNoPath:
                    continue

        return waves

    @staticmethod
    def get_flood_waves_without_equivalence(joined_graph: nx.DiGraph) -> list:
        """
        This function returns all the 'elements' of the theoretical flood wave equivalence classes (so for given
        start and end nodes it takes all paths between them)
        :param nx.DiGraph joined_graph: the graph
        :return list: paths
        """
        components = list(nx.weakly_connected_components(joined_graph))

        waves = []
        for comp in components:
            final_pairs = AnalysisHandler.get_final_pairs(joined_graph=joined_graph, comp=list(comp))

            for start, end in final_pairs:
                try:
                    wave = nx.all_shortest_paths(joined_graph, start, end)
                    waves.append(list(wave))
                except nx.NetworkXNoPath:
                    continue

        return waves

    @staticmethod
    def connected_components_iter(
                joined_graph: nx.DiGraph,
                start_station: str,
                end_station: str,
                func: Callable
                  ) -> list:
        """
        Iterates through all the connected components within the input graph joined_graph.
        It gathers all the start and end nodes that are (weakly)reachable within the same connected component.
        Within the iteration of each connected component, it executes a callable function which is given as input.
        (It should be a calculation which could be executed using the same inputs as given for this function)
        The results are gathered into a list and returned.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the station from which you want to get the reachable end nodes.
        :param str end_station: The ID of the desired end station.
        :param Callable func:
        :return list: The results organized into a list.
        """
        
        connected_components = [
            list(x)
            for x in nx.weakly_connected_components(joined_graph)
        ]
        counted_quantity = []
        for sub_connected_component in connected_components:
            start_nodes = [
                node
                for node in sub_connected_component
                if node[0] == start_station
            ]
            end_nodes = [
                node
                for node in sub_connected_component
                if node[0] == end_station
            ]
            counted_quantity.extend(func(j_graph=joined_graph,
                                         start_nodes=start_nodes,
                                         end_nodes=end_nodes))
            
        return counted_quantity 

    @staticmethod
    def count_waves(joined_graph: nx.DiGraph,
                    start_station: str,
                    end_station: str
                    ) -> int:
        """
        Returns the number of flood waves which impacted the start_station and reached the end_station as well.
        If there were branching(s), then all the branches that reach the end_station will be counted.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station.
        :param str end_station: The ID of the desired end station.
        :return int: The number of flood waves which impacted the start_station and reached the end_station
        """

        full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
                                                                         start_station=start_station,
                                                                         end_station=end_station)

        flood_waves = GraphAnalysis.get_flood_waves(joined_graph=full_from_start_to_end)

        return len(flood_waves)

    @staticmethod
    def propagation_time(joined_graph: nx.DiGraph,
                         start_station: str,
                         end_station: str
                         ) -> float:
        """
        Returns the average propagation time of flood waves between the two selected stations unweighted,
        meaning that no matter how many paths are between the same two vertices, the propagation time value
        will be only counted in one.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station
        :return float: The average propagation time of flood waves in joined_graph between the two given stations.
        """
        full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
                                                                         start_station=start_station,
                                                                         end_station=end_station)

        flood_waves = GraphAnalysis.get_flood_waves(joined_graph=full_from_start_to_end)

        prop_times = []
        for wave in flood_waves:
            start = wave[0][1]
            end = wave[-1][1]

            d1 = datetime.strptime(start, "%Y-%m-%d")
            d2 = datetime.strptime(end, "%Y-%m-%d")

            delta = d2 - d1
            prop_times.append(delta.days)

        return np.average(prop_times)

    @staticmethod
    def propagation_time_weighted(joined_graph: nx.DiGraph,
                                  start_station: str,
                                  end_station: str
                                  ) -> int:
        """
        Returns the weighted average propagation time of flood waves between the two selected stations. Each time value
        is weighted by the number of paths with that given propagation time.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station, which is not reached by the flood waves
        :return float: The weighted average propagation time of flood waves in joined_graph between
        the two given stations.
        """
        full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
                                                                         start_station=start_station,
                                                                         end_station=end_station)

        classes = GraphAnalysis.get_flood_waves_without_equivalence(joined_graph=full_from_start_to_end)

        prop_times = []
        for paths in classes:
            prop_times_in_classes = []
            for path in paths:
                start = path[0][1]
                end = path[-1][1]

                d1 = datetime.strptime(start, "%Y-%m-%d")
                d2 = datetime.strptime(end, "%Y-%m-%d")

                delta = d2 - d1
                prop_times_in_classes.append(delta.days)

            prop_times.append(prop_times_in_classes)

        weighted_prop_times = []
        for x in prop_times:
            weighted_prop_times.extend([a / len(x) for a in x])

        return np.average(weighted_prop_times)

    @staticmethod
    def count_unfinished_waves(joined_graph: nx.DiGraph,
                               start_station: str,
                               end_station: str
                               ) -> int:
        """
        Returns the number of flood waves which impacted the start_station, but did not reach the end_station.
        If there were branching(s), then all the branches will be counted.

        :param nx.DiGraph joined_graph: full composed graph of the desired time interval
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station, which is not reached by the flood waves
        :return int: The number of flood waves which impacted the start_station but did not reach the end_station
        """

        select_all_in_interval = Selection.select_only_in_interval(joined_graph=joined_graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station)

        classes = GraphAnalysis.get_flood_waves_without_equivalence(joined_graph=select_all_in_interval)

        final_flood_waves = []
        for paths in classes:
            for path in paths:
                stations = []
                for node in path:
                    stations.append(node[0])

                if any(start_station == elem for elem in stations) and all(end_station != elem for elem in stations):
                    final_flood_waves.append(path)

        return len(final_flood_waves)

    @staticmethod
    def create_flood_map(joined_graph: nx.DiGraph,
                         river_section_gauges: list
                         ) -> nx.DiGraph:
        """
        Returns the number of flood waves which impacted the start_station and reached the end_station as well.
        If there were branching(s), then all the branches that reach the end_station will be counted.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param list river_section_gauges:
        :return int: The number of flood waves which impacted the start_station and reached the end_station
        """
        flood_map = nx.DiGraph()
        river_sections = [(x, y) for x, y in zip(river_section_gauges, river_section_gauges[1:])]
        flood_map_edges = []

        def func(j_graph, start_nodes, end_nodes):
            edges = []
            for start in start_nodes:
                for end in end_nodes:
                    try:
                        paths = [p for p in nx.all_shortest_paths(j_graph, start, end)]
                        edges.append((start, end, len(paths)))
                    except nx.NetworkXNoPath:
                        continue
            return edges

        for section in river_sections:
            flood_map_edges.extend(GraphAnalysis.connected_components_iter(joined_graph=joined_graph,
                                                                           start_station=section[0],
                                                                           end_station=section[1],
                                                                           func=func))
        flood_map.add_weighted_edges_from(ebunch_to_add=flood_map_edges)
        return flood_map

    @staticmethod
    def get_branching(joined_graph: nx.DiGraph) -> list:
        """
        This function returns components in which every node has at most one parent (called a branching)
        :param nx.DiGraph joined_graph: the graph
        :return list: the branching
        """
        branching = nx.dag_to_branching(joined_graph)
        sources = defaultdict(set)
        for v, source in branching.nodes(data="source"):
            sources[source].add(v)
        for source, nodes in sources.items():
            for v in nodes:
                branching.nodes[v].update(joined_graph.nodes[source])

        final_comps = []
        for comp in list(nx.weakly_connected_components(branching)):
            comp = list(comp)
            temp_list = []
            for node in comp:
                temp_list.append(branching.nodes[node]['source'])
            final_comps.append(temp_list)

        return final_comps

    @staticmethod
    def calculate_all_velocities(joined_graph: nx.DiGraph, river_kms: pd.Series) -> list:
        """
        This function calculates the velocity of all flood waves in the input graph
        :param nx.DiGraph joined_graph: the graph
        :param pd.Series river_kms: river kilometers of the gauges
        :return list: velocities in a list
        """
        branching = GraphAnalysis.get_branching(joined_graph=joined_graph)

        velocities = []
        for branch in branching:
            start_node, end_node = \
                AnalysisHandler.get_start_and_end_node_of_slowest_and_longest_flood_wave(branch=branch)

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
