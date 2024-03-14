import networkx as nx

from src.core.flood_wave_handler import FloodWaveHandler


class FloodWaveExtractor:
    def __init__(self, joined_graph: nx.DiGraph):
        self.joined_graph = joined_graph
        self.flood_waves = None

    def get_flood_waves(self):
        """
        This function returns the actual flood waves in the graph
        :return list: list of lists of the flood wave nodes
        """
        components = list(nx.weakly_connected_components(self.joined_graph))

        waves = []
        for comp in components:
            final_pairs = FloodWaveHandler.get_final_pairs(joined_graph=self.joined_graph, comp=list(comp))

            for start, end in final_pairs:
                try:
                    wave = nx.shortest_path(self.joined_graph, start, end)
                    waves.append(wave)
                except nx.NetworkXNoPath:
                    continue

        self.flood_waves = waves

    def get_flood_waves_without_equivalence(self):
        """
        This function returns all the 'elements' of the theoretical flood wave equivalence classes (so for given
        start and end nodes it takes all paths between them)
        :return list: paths
        """
        components = list(nx.weakly_connected_components(self.joined_graph))

        waves = []
        for comp in components:
            final_pairs = FloodWaveHandler.get_final_pairs(joined_graph=self.joined_graph, comp=list(comp))

            for start, end in final_pairs:
                try:
                    wave = nx.all_shortest_paths(self.joined_graph, start, end)
                    waves.append(list(wave))
                except nx.NetworkXNoPath:
                    continue

        self.flood_waves = waves

    @staticmethod
    def get_flood_waves_from_start_to_end(waves: list,
                                          start_station: str,
                                          end_station: str,
                                          equivalence: bool) -> list:
        """
        Selects only those flood waves that impacted both the start_station and end_station
        :param list waves: list of all the flood waves
        :param str start_station: the start station
        :param str end_station: the end station
        :param bool equivalence: True if we only consider one element of the equivalence classes, False otherwise
        :return list: full flood waves
        """
        if equivalence:
            final_waves = []
            for path in waves:
                if start_station == path[0][0] and end_station == path[-1][0]:
                    final_waves.append(path)

        else:
            final_waves = []
            for paths in waves:
                if start_station == paths[0][0][0] and end_station == paths[0][-1][0]:
                    final_waves.append(paths)

        return final_waves
