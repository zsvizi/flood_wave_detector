import itertools

import networkx as nx


class FloodWaveHandler:
    """
    This is a helper class for FloodWaveExtractor and GraphAnalysis
    """

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
