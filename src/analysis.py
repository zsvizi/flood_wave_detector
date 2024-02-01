from datetime import datetime
from typing import Union, Callable

import networkx as nx

from src.flood_wave_data import FloodWaveData


class Analysis:
    """This is an analysis class for flood waves.

    Any method that does calculation or information extraction on the already existing flood wave graph structure
    belongs here.
    """
    def __init__(self, gauges: Union[list, None] = None) -> None:
        """
        Constructor for Analysis class

        :param Union[list, None] gauges: The gauges used for the analysis.
        """
        self.data = FloodWaveData()
        self.gauges = []
        if gauges is not None:
            self.gauges = gauges
        else:
            self.gauges = self.data.gauges

    @staticmethod
    def connected_components_iter(
                joined_graph: nx.DiGraph,
                start_station: int,
                end_station: int,
                func: Callable
                  ) -> list:
        """
        Iterates through all the connected components within the input graph joined_graph.
        It gathers all the start and end nodes that are (weakly)reachable within the same connected component.
        Within the iteration of each connected component, it executes a callable function which is given as input.
        (It should be a calculation which could be executed using the same inputs as given for this function)
        The results are gathered into a list and returned.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the station from which you want to get the reachable end nodes.
        :param int end_station: The ID of the desired end station.
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
                if int(node[0]) == start_station
            ]
            end_nodes = [
                node
                for node in sub_connected_component
                if int(node[0]) == end_station
            ]
            counted_quantity.extend(func(j_graph=joined_graph,
                                         start_nodes=start_nodes,
                                         end_nodes=end_nodes))
            
        return counted_quantity 

    def count_waves(
            self,
            joined_graph: nx.DiGraph,
            start_station: int,
            end_station: int
    ) -> int:
        """
        Returns the number of flood waves which impacted the start_station and reached the end_station as well.
        If there were branching(s), then all the branches that reach the end_station will be counted.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the desired start station.
        :param int end_station: The ID of the desired end station.
        :return int: The number of flood waves which impacted the start_station and reached the end_station
        """  
        def func(j_graph, start_nodes, end_nodes):
            waves = []
            for start in start_nodes:
                for end in end_nodes:
                    try:
                        nx.shortest_path(j_graph, start, end)
                        waves.append((nx.shortest_path(j_graph, start, end)[0],
                                      nx.shortest_path(j_graph, start, end)[-1]))
                    except nx.NetworkXNoPath:
                        continue
            return waves
        return len(set(self.connected_components_iter(joined_graph=joined_graph,
                                                      start_station=start_station,
                                                      end_station=end_station,
                                                      func=func)))

    def propagation_time(
            self,
            joined_graph: nx.DiGraph,
            start_station: int,
            end_station: int
    ) -> int:
        """
        Returns the average propagation time of flood waves between the two selected stations unweighted,
        meaning that no matter how many paths are between the same two vertices, the propagation time value
        will be only counted in once.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the desired start station
        :param int end_station: The ID of the last station, which is not reached by the flood waves
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
        
        prop_times_total = self.connected_components_iter(joined_graph=joined_graph,
                                                          start_station=start_station,
                                                          end_station=end_station,
                                                          func=func)
        if sum(prop_times_total):
            avg_prop_time = sum(prop_times_total) / len(prop_times_total)
        else:
            avg_prop_time = 0
  
        return avg_prop_time

    def propagation_time_weighted(
            self,
            joined_graph: nx.DiGraph,
            start_station: int,
            end_station: int
    ) -> int:
        """
        Returns the weighted average propagation time of flood waves between the two selected stations. Each time value
        is weighted by the number of paths with that given propagation time.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the desired start station
        :param int end_station: The ID of the last station, which is not reached by the flood waves
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
            
        prop_times_total = self.connected_components_iter(joined_graph=joined_graph,
                                                          start_station=start_station,
                                                          end_station=end_station,
                                                          func=func)
        if sum(prop_times_total):
            avg_prop_time = sum(prop_times_total) / len(prop_times_total)
        else:
            avg_prop_time = 0
        
        return avg_prop_time

    def count_unfinished_waves(self,
                               joined_graph: nx.DiGraph,
                               start_station: int,
                               end_station: int
                               ) -> int:
        """
        Returns the number of flood waves which impacted the start_station, but did not reach the end_station.
        If there were branching(s), then all the branches will be counted.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the desired start station
        :param int end_station: The ID of the last station, which is not reached by the flood waves
        :return int: The number of flood waves which impacted the start_station but did not reach the end_station
        """
        # Some used nx features are not implemented for DiGraphs
        joined_graph = joined_graph.to_undirected()
        # First we select the gauges between start_station and end_station
        start_index = self.gauges.index(start_station)
        end_index = self.gauges.index(end_station)
        gauges = self.gauges[start_index:end_index + 1]

        # We select the nodes of the graph, where the gauge (node[0]) is in the already existing gauges list
        nodes = [
            node
            for node in joined_graph.nodes
            if int(node[0]) in gauges
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
                if int(node[0]) == start_station
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
                    if int(end_node[0]) != end_station:
                        unfinished_waves += len(paths)

        return unfinished_waves

    def create_flood_map(
            self,
            joined_graph: nx.DiGraph,
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
            flood_map_edges.extend(self.connected_components_iter(joined_graph=joined_graph, start_station=section[0],
                                                                  end_station=section[1], func=func))
        flood_map.add_weighted_edges_from(ebunch_to_add=flood_map_edges)
        return flood_map

    @staticmethod
    def filter_by_gauge(graph,
                        gauge: str
                        ):
        comps = list(nx.weakly_connected_components(graph))
        edges = graph.edges()
        comps_copy = comps.copy()
        edges = list(edges)
        edges_copy = edges.copy()

        for comp in comps_copy:
            if not any(gauge == elem for elem in [i[0] for i in [list(ele) for ele in list(comp)]]):
                comps.remove(comp)
        for i in range(len(comps)):
            comps[i] = list(comps[i])
        nodes = [item for sublist in comps for item in sublist]

        for edge in edges_copy:
            for comp in comps:
                if edge[0] in comp:
                    break
                if comp == comps[-1]:
                    edges.remove(edge)

        g = nx.DiGraph()
        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        return g

    @staticmethod
    def filter_multiple_gauges(graph,
                               start_gauge: str,
                               end_gauge: str,
                               ):
        """
        This method filters an interval of gauges. Any component starting in the interval will be displayed,
        otherwise deleted.
        """
        # gauges = ["1514", "1515", "1516", "1518", "1520", "1521", "1719", "1720", "1721", "2541", "1722", "1723",
        #           "2543", "2040", "2041", "2042", "2046", "2048", "2271", "2272", "2274", "2275", "210888",
        #           "210896", "210900"]
        gauges = [str(i) for i in range(1, 15)]
        comps = list(nx.weakly_connected_components(graph))
        edges = graph.edges()
        comps_copy = comps.copy()
        edges = list(edges)
        edges_copy = edges.copy()

        filtered_gauges = gauges[gauges.index(start_gauge):gauges.index(end_gauge) + 1]

        for comp in comps_copy:
            list_of_bools = []
            for fg in filtered_gauges:
                x = any(fg == elem for elem in [i[0] for i in [list(ele) for ele in list(comp)]])
                list_of_bools.append(x)

            if not any(list_of_bools):
                comps.remove(comp)

        comps_copy = comps.copy()
        gauges_to_delete = gauges[0:gauges.index(start_gauge)]
        for comp in comps_copy:
            comp_copy = comp.copy()
            for elem in comp_copy:
                if any(gtd in str(elem) for gtd in gauges_to_delete):
                    comps[comps.index(comp)].remove(elem)

        for i in range(len(comps)):
            comps[i] = list(comps[i])
        nodes = [item for sublist in comps for item in sublist]

        for edge in edges_copy:
            for comp in comps:
                if edge[0] in comp:
                    break
                if comp == comps[-1]:
                    edges.remove(edge)

        g = nx.DiGraph()
        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        return g

    @staticmethod
    def filter_by_water_level(graph,
                              gauge: str,
                              positions,
                              node_colors,
                              ):
        comps = list(nx.weakly_connected_components(graph))
        edges = graph.edges()
        comps_copy = comps.copy()
        edges = list(edges)
        edges_copy = edges.copy()
        for comp in comps_copy:
            comp_list = list(comp)
            i0_list = []
            gauge_in_comp = []
            for i in comp_list:
                i0_list.append(i[0])
                if gauge == i[0]:
                    gauge_in_comp.append(i)

            if not any(gauge == elem for elem in i0_list):
                comps.remove(comp)
            else:
                colors_of_gauge = []
                for elem in gauge_in_comp:
                    idx = list(positions.keys()).index(elem)
                    colors_of_gauge.append(node_colors[idx])

                if not any("red" == color for color in colors_of_gauge):
                    comps.remove(comp)

        for i in range(len(comps)):
            comps[i] = list(comps[i])
        nodes = [item for sublist in comps for item in sublist]

        for edge in edges_copy:
            for comp in comps:
                if edge[0] in comp:
                    break
                if comp == comps[-1]:
                    edges.remove(edge)

        g = nx.DiGraph()
        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        return g
