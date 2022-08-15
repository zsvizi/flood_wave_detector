import networkx as nx

from src.flood_wave_data import FloodWaveData


class Analysis:
    """This is an analysis class for flood waves.

    Any method that does calculation or information extraction on the already existing flood wave graph structure
    belongs here.
    """
    def __init__(self) -> None:
        self.data = FloodWaveData()

    @staticmethod
    def count_waves(
            joined_graph: nx.Graph,
            start_station: int,
            end_station: int
    ) -> int:
        """
        Returns the number of flood waves which impacted the start_station and reached the end_station as well.
        If there were branching(s), then all the branches that reach the end_station will be counted.

        :param joined_graph: The full composed graph of the desired time interval.
        :param start_station: The ID of the desired start station.
        :param end_station: The ID of the desired end station.
        :return:
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
        
        start_index = self.data.gauges.index(start_station)
        end_index = self.data.gauges.index(end_station)
        gauges = self.data.gauges[start_index:end_index + 1]
        print(gauges)

        nodes = [node for node in joined_graph if int(node[0]) in gauges]
        print(nodes)

        subgraph = joined_graph.subgraph(nodes)
        # print(subgraph)
        
        connected_components = [
            list(x)
            for x
            in nx.connected_components(subgraph)
        ]
        print(connected_components)
        
        unfinished_waves = 0
            
        for sub_connected_component in connected_components:

            print(sub_connected_component)

            start_nodes = [
                node
                for node in sub_connected_component
                if int(node[0]) == start_station
            ]

            component_gauges = [
                x[0]
                for x in sub_connected_component
            ]
            print(component_gauges)

            component_gauges_ordered = [
                str(x)
                for x in gauges
                if str(x) in component_gauges
            ]
            print(component_gauges_ordered)

            end_nodes = [
                node
                for node in sub_connected_component
                if node[0] == component_gauges_ordered[-1]
            ]
            print(end_nodes)
            
            for start_node in start_nodes:
                for end_node in end_nodes:
                    paths = [
                        list(x)
                        for x in nx.all_shortest_paths(joined_graph, source=start_node, target=end_node)
                    ]
                    print(paths)
                    
                    if int(end_node[0]) != end_station:
                        unfinished_waves += len(paths)

        return unfinished_waves
