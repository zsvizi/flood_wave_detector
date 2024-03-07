import itertools
from collections import defaultdict
from datetime import datetime
from typing import Callable

import networkx as nx
import pandas as pd

from src.analysis.analysis_handler import AnalysisHandler


class GraphAnalysis:
    """This is an analysis class for flood waves.

    Any method that does calculation or information extraction on the already existing flood wave graph structure
    belongs here.
    """

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

        flood_waves = GraphAnalysis.get_flood_waves(joined_graph=joined_graph)

        selected = []
        for fw in flood_waves:
            stations = []
            for node in fw:
                stations.append(node[0])

            if any(start_station == elem for elem in stations) and any(end_station == elem for elem in stations):
                selected.append(fw)

        only_one = []
        for fw in selected:
            stations = []
            for node in fw:
                stations.append(node[0])

            interval = fw[stations.index(start_station):stations.index(end_station)+1]
            if interval not in only_one:
                only_one.append(interval)

        return len(only_one)

    @staticmethod
    def propagation_time(joined_graph: nx.DiGraph,
                         start_station: str,
                         end_station: str
                         ) -> int:
        """
        Returns the average propagation time of flood waves between the two selected stations unweighted,
        meaning that no matter how many paths are between the same two vertices, the propagation time value
        will be only counted in once.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station, which is not reached by the flood waves
        :return float: The average propagation time of flood waves in joined_graph between the two given stations.
        """
        def func(j_graph, start_nodes, end_nodes):
            prop_times = []

            for start in start_nodes:
                start_date = datetime.strptime(start[1], '%Y-%m-%d').date()
                for end in end_nodes:
                    try:
                        nx.shortest_path(j_graph, start, end)
                        end_date = datetime.strptime(end[1], '%Y-%m-%d').date()
                        diff = (end_date - start_date).days
                        prop_times.append(diff)
                    except nx.NetworkXNoPath:
                        continue
            return prop_times
        
        prop_times_total = GraphAnalysis.connected_components_iter(joined_graph=joined_graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station,
                                                                   func=func)
        if sum(prop_times_total):
            avg_prop_time = sum(prop_times_total) / len(prop_times_total)
        else:
            avg_prop_time = 0
  
        return avg_prop_time

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
        def func(j_graph, start_nodes, end_nodes):
            prop_times = []
            for start in start_nodes:
                start_date = datetime.strptime(start[1], '%Y-%m-%d').date()
                for end in end_nodes:
                    try:
                        paths = [p for p in nx.all_shortest_paths(j_graph, start, end)]
                        end_date = datetime.strptime(end[1], '%Y-%m-%d').date()
                        diff = [(end_date - start_date).days] * len(paths)
                        prop_times.extend(diff)
                    except nx.NetworkXNoPath:
                        continue
            return prop_times
            
        prop_times_total = GraphAnalysis.connected_components_iter(joined_graph=joined_graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station,
                                                                   func=func)
        if sum(prop_times_total):
            avg_prop_time = sum(prop_times_total) / len(prop_times_total)
        else:
            avg_prop_time = 0
        
        return avg_prop_time

    @staticmethod
    def count_unfinished_waves(joined_graph: nx.DiGraph,
                               gauges: list,
                               start_station: str,
                               end_station: str
                               ) -> int:
        """
        Returns the number of flood waves which impacted the start_station, but did not reach the end_station.
        If there were branching(s), then all the branches will be counted.

        :param nx.DiGraph joined_graph: full composed graph of the desired time interval
        :param list gauges: list of gauges
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station, which is not reached by the flood waves
        :return int: The number of flood waves which impacted the start_station but did not reach the end_station
        """
        # Some used nx features are not implemented for DiGraphs
        joined_graph = joined_graph.to_undirected()
        # First we select the gauges between start_station and end_station
        start_index = gauges.index(start_station)
        end_index = gauges.index(end_station)
        gauges = gauges[start_index:end_index + 1]

        # We select the nodes of the graph, where the gauge (node[0]) is in the already existing gauges list
        nodes = [
            node
            for node in joined_graph.nodes
            if float(node[0]) in gauges
        ]
        # Creating the subgraph induced on the nodes list
        subgraph = joined_graph.subgraph(nodes)
        
        # We need the connected components of subgraph, but the components must have at least two vertices
        connected_components = [
            list(x)
            for x in nx.connected_components(subgraph)
            if len(list(x)) >= 2
        ]
       
        unfinished_waves = 0
            
        # We iterate through every connected component of subgraph
        for sub_connected_component in connected_components:

            # If the gauge (node[0]) of a node is the start station, we will count waves from that node
            start_nodes = [
                node
                for node in sub_connected_component
                if node[0] == start_station
            ]

            # We need to select which gauges are included in the connected component
            component_gauges = [
                x[0]
                for x in sub_connected_component
            ]
            
            # Ordering the component's gauges with respect to river km
            component_gauges_ordered = [
                str(x)
                for x in gauges
                if str(x) in component_gauges
            ]
            # A node is end node if its gauge (node[0]) is the last element of the ordered list
            end_nodes = [
                node
                for node in sub_connected_component
                if node[0] == component_gauges_ordered[-1]
            ]
            
            # Counting the number of waves between all start and end nodes
            for start_node in start_nodes:
                for end_node in end_nodes:

                    paths = [
                        list(x)
                        for x in nx.all_shortest_paths(joined_graph, source=start_node, target=end_node)
                    ]
                    
                    # We need only those waves, when the last station is not the end station (a. k. a. unfinished wave)
                    if end_node[0] != end_station:
                        unfinished_waves += len(paths)

        return unfinished_waves

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
            comp = list(comp)

            possible_end_nodes = []
            for node in comp:
                in_deg = joined_graph.in_degree(node)
                out_deg = joined_graph.out_degree(node)

                if in_deg == 0 or out_deg == 0:
                    possible_end_nodes.append(node)

            cartesian_pairs = list(itertools.product(possible_end_nodes, repeat=2))

            final_pairs = [(x, y) for x, y in cartesian_pairs if float(x[0]) > float(y[0])]

            for start, end in final_pairs:
                try:
                    wave = nx.shortest_path(joined_graph, start, end)
                    waves.append(wave)
                except nx.NetworkXNoPath:
                    continue

        return waves
