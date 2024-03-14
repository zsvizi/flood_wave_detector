import networkx as nx

from src.core.flood_wave_extractor import FloodWaveExtractor
from src.selection.selection import Selection


class FloodMapCreator:
    def __init__(self, joined_graph: nx.DiGraph):
        self.joined_graph = joined_graph

    def create_flood_map(self, river_section_stations: list, sorted_stations: list) -> nx.DiGraph:
        """
        Creates a flood map of the original graph
        :param list river_section_stations: edge stations of the desired sections
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return nx.DiGraph: flood map as a directed graph
        """
        flood_map = nx.DiGraph()
        river_sections = [(x, y) for x, y in zip(river_section_stations, river_section_stations[1:])]

        edges = []
        for section in river_sections:
            start_station = section[0]
            end_station = section[1]

            full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=self.joined_graph,
                                                                             start_station=start_station,
                                                                             end_station=end_station,
                                                                             sorted_stations=sorted_stations)

            extractor = FloodWaveExtractor(joined_graph=full_from_start_to_end)
            extractor.get_flood_waves_without_equivalence()
            classes = extractor.flood_waves

            full_waves = FloodWaveExtractor.get_flood_waves_from_start_to_end(waves=classes,
                                                                              start_station=start_station,
                                                                              end_station=end_station,
                                                                              equivalence=False)

            for paths in full_waves:
                start_node = paths[0][0]
                end_node = paths[0][-1]
                amount = len(paths)

                edges.append((start_node, end_node, amount))

        flood_map.add_weighted_edges_from(ebunch_to_add=edges)

        return flood_map
