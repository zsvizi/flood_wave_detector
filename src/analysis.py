import networkx as nx


class Analysis:
    def __init__(self) -> None:
        pass

    @staticmethod
    def count_waves(
            joined_graph: nx.Graph,
            start_station: int,
            end_station: int
    ) -> int:

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
