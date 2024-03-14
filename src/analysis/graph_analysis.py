from collections import defaultdict
from datetime import datetime

import networkx as nx
import numpy as np

from src.core.flood_wave_extractor import FloodWaveExtractor
from src.data.dataloader import Dataloader
from src.selection.selection import Selection


class GraphAnalysis:
    """This is an analysis class for flood waves.

    Any method that does calculation or information extraction on the already existing flood wave graph structure
    belongs here.
    """

    @staticmethod
    def count_waves(joined_graph: nx.DiGraph,
                    start_station: str,
                    end_station: str,
                    sorted_stations: list) -> int:
        """
        Returns the number of flood waves which impacted the start_station and reached the end_station as well.
        If there were branching(s), then all the branches that reach the end_station will be counted.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station.
        :param str end_station: The ID of the desired end station.
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return int: The number of flood waves which impacted the start_station and reached the end_station
        """

        full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
                                                                         start_station=start_station,
                                                                         end_station=end_station,
                                                                         sorted_stations=sorted_stations)

        extractor = FloodWaveExtractor(joined_graph=full_from_start_to_end)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        full_waves = FloodWaveExtractor.get_flood_waves_from_start_to_end(waves=flood_waves,
                                                                          start_station=start_station,
                                                                          end_station=end_station,
                                                                          equivalence=True)

        return len(full_waves)

    @staticmethod
    def propagation_time(joined_graph: nx.DiGraph,
                         start_station: str,
                         end_station: str,
                         sorted_stations: list) -> float:
        """
        Returns the average propagation time of flood waves between the two selected stations unweighted,
        meaning that no matter how many paths are between the same two vertices, the propagation time value
        will be only counted in one.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return float: The average propagation time of flood waves in joined_graph between the two given stations.
        """
        full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
                                                                         start_station=start_station,
                                                                         end_station=end_station,
                                                                         sorted_stations=sorted_stations)

        extractor = FloodWaveExtractor(joined_graph=full_from_start_to_end)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        full_waves = FloodWaveExtractor.get_flood_waves_from_start_to_end(waves=flood_waves,
                                                                          start_station=start_station,
                                                                          end_station=end_station,
                                                                          equivalence=True)

        prop_times = []
        for wave in full_waves:
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
                                  end_station: str,
                                  sorted_stations: list) -> int:
        """
        Returns the weighted average propagation time of flood waves between the two selected stations. Each time value
        is weighted by the number of paths with that given propagation time.

        :param nx.DiGraph joined_graph: The full composed graph of the desired time interval.
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station, which is not reached by the flood waves
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return float: The weighted average propagation time of flood waves in joined_graph between
        the two given stations.
        """
        full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
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

        prop_times = []
        for paths in full_waves:
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
                               end_station: str,
                               sorted_stations: list) -> int:
        """
        Returns the number of flood waves which impacted the start_station, but did not reach the end_station.
        If there were branching(s), then all the branches will be counted.

        :param nx.DiGraph joined_graph: full composed graph of the desired time interval
        :param str start_station: The ID of the desired start station
        :param str end_station: The ID of the last station, which is not reached by the flood waves
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return int: The number of flood waves which impacted the start_station but did not reach the end_station
        """

        select_all_in_interval = Selection.select_only_in_interval(joined_graph=joined_graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station,
                                                                   sorted_stations=sorted_stations)

        extractor = FloodWaveExtractor(joined_graph=select_all_in_interval)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        final_flood_waves = []
        for path in flood_waves:
            stations = []
            for node in path:
                stations.append(node[0])

            if any(start_station == elem for elem in stations) and all(end_station != elem for elem in stations):
                final_flood_waves.append(path)

        return len(final_flood_waves)

    @staticmethod
    def create_flood_map(joined_graph: nx.DiGraph, river_section_stations: list, sorted_stations: list) -> nx.DiGraph:
        """
        Creates a flood map of the original graph
        :param nx.DiGraph joined_graph: the graph
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

            full_from_start_to_end = Selection.select_full_from_start_to_end(joined_graph=joined_graph,
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
    def calculate_all_velocities(joined_graph: nx.DiGraph) -> list:
        """
        This function calculates the velocity of all flood waves in the input graph
        :param nx.DiGraph joined_graph: the graph
        :return list: velocities in a list
        """
        meta = Dataloader.get_metadata()
        river_kms = meta["river_km"]

        extractor = FloodWaveExtractor(joined_graph=joined_graph)
        extractor.get_flood_waves()
        flood_waves = extractor.flood_waves

        velocities = []
        for wave in flood_waves:
            start_node = wave[0]
            end_node = wave[-1]

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
