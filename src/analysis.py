from typing import Union

import networkx as nx

from src.flood_wave_data import FloodWaveData


class Analysis:
    """This is an analysis class for flood waves.

    Any method that does calculation or information extraction on the already existing flood wave graph structure
    belongs here.
    """
    def __init__(self, gauges: Union[list, None] = None) -> None:
        self.data = FloodWaveData()
        self.gauges = []
        if gauges is not None:
            self.gauges = gauges
        else:
            self.gauges = self.data.gauges

    @staticmethod
    def count_waves(
            joined_graph: nx.Graph,
            start_station: int,
            end_station: int
    ) -> int:
        """
        Returns the number of flood waves which impacted the start_station and reached the end_station as well.
        If there were branching(s), then all the branches that reach the end_station will be counted.

        :param nx.Graph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the desired start station.
        :param int end_station: The ID of the desired end station.
        :return int: The number of flood waves which impacted the start_station and reached the end_station
        """

        connected_components = [
            list(x)
            for x in nx.connected_components(joined_graph)
        ]

        total_waves = 0
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

            for start in start_nodes:
                for end in end_nodes:
                    paths = [
                        list(x)
                        for x in nx.all_shortest_paths(joined_graph, source=start, target=end)]
                    total_waves += len(paths)
        return total_waves

    def count_unfinished_waves(self,
                               joined_graph: nx.Graph,
                               start_station: int,
                               end_station: int
                               ) -> int:
        """
        Returns the number of flood waves which impacted the start_station, but did not reach the end_station.
        If there were branching(s), then all the branches will be counted.

        :param nx.Graph joined_graph: The full composed graph of the desired time interval.
        :param int start_station: The ID of the desired start station
        :param int end_station: The ID of the last station, which is not reached by the flood waves
        :return int: The number of flood waves which impacted the start_station but did not reach the end_station
        """

        # First we select the gauges between start_station and end_station
        start_index = self.gauges.index(start_station)
        end_index = self.gauges.index(end_station)
        gauges = self.gauges[start_index:end_index + 1]
        print(gauges)

        # We select the nodes of the graph, where the gauge (node[0]) is in the already existing gauges list
        nodes = [
            node
            for node in joined_graph.nodes
            if int(node[0]) in gauges
        ]
        print(nodes)

        # Creating the subgraph induced on the nodes list
        subgraph = joined_graph.subgraph(nodes)
        print(subgraph)
        
        # We need the connected components of subgraph, but the components must have at least two vertices
        connected_components = [
            list(x)
            for x in nx.connected_components(subgraph)
            if len(list(x)) >= 2
        ]
        print(connected_components)
        
        unfinished_waves = 0
            
        # We iterate through every connected component of subgraph
        for sub_connected_component in connected_components:

            print(sub_connected_component)

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
            print(component_gauges)

            # Ordering the component's gauges with respect to river km
            component_gauges_ordered = [
                str(x)
                for x in gauges
                if str(x) in component_gauges
            ]
            print(component_gauges_ordered)

            # A node is end node if its gauge (node[0]) is the last element of the ordered list
            end_nodes = [
                node
                for node in sub_connected_component
                if node[0] == component_gauges_ordered[-1]
            ]
            print(end_nodes)
            
            # Counting the number of waves between all start and end nodes
            for start_node in start_nodes:
                for end_node in end_nodes:

                    paths = [
                        list(x)
                        for x in nx.all_shortest_paths(joined_graph, source=start_node, target=end_node)
                    ]
                    print(paths)
                    
                    # We need only those waves, when the last station is not the end station (a. k. a. unfinished wave)
                    if int(end_node[0]) != end_station:
                        unfinished_waves += len(paths)

        return unfinished_waves
