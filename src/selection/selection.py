import copy

import networkx as nx

from src.core.flood_wave_extractor import FloodWaveExtractor
from src.selection.selection_handler import SelectionHandler


class Selection:
    """
    This class contains selection functions for the final graph.
    """

    @staticmethod
    def select_by_station(joined_graph: nx.DiGraph, station: str) -> nx.DiGraph:
        """
        This function selects weakly connected components that have the given gauge as a node.

        :param nx.DiGraph joined_graph: graph
        :param str station: gauge number to be selected as a string
        :return nx.DiGraph: graph that contains only components which have gauge as a node
        """

        comps = list(nx.weakly_connected_components(joined_graph))
        edges = joined_graph.edges()
        comps_copy = comps.copy()
        edges = list(edges)

        comps_new = []
        for comp in comps_copy:
            if SelectionHandler.is_gauge_in_comp(gauge=station, comp_list=list(comp)):
                comps_new.append(comp)

        nodes_filtered, edges_filtered = SelectionHandler.nodes_and_edges(comps=comps_new, edges=edges)

        g = nx.DiGraph()
        g.add_nodes_from(nodes_filtered)
        g.add_edges_from(edges_filtered)

        return g

    @staticmethod
    def select_only_in_interval(joined_graph: nx.DiGraph,
                                start_station: str,
                                end_station: str,
                                sorted_stations: list) -> nx.DiGraph:
        """
        This function selects an interval of gauges. Each component's intersection with the given interval
        will be displayed.

        :param nx.DiGraph joined_graph: graph
        :param str start_station: first gauge of the interval as a string
        :param str end_station: last gauge of the interval as a string
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return nx.DiGraph: graph that contains only components that intersect with the interval
        """

        filtered = Selection.select_intersecting_with_interval(joined_graph=joined_graph,
                                                               start_station=start_station,
                                                               end_station=end_station,
                                                               sorted_stations=sorted_stations)

        comps = list(nx.weakly_connected_components(filtered))

        gauges = SelectionHandler.get_gauges(comps=comps)

        filtered_stations = sorted_stations[sorted_stations.index(start_station):sorted_stations.index(end_station) + 1]

        stations_to_delete = [x for x in gauges if x not in filtered_stations]
        nodes = copy.deepcopy(list(filtered.nodes()))
        for node in nodes:
            if any(node[0] == station for station in stations_to_delete):
                filtered.remove_node(node)

        return filtered

    @staticmethod
    def select_intersecting_with_interval(joined_graph: nx.DiGraph,
                                          start_station: str,
                                          end_station: str,
                                          sorted_stations: list) -> nx.DiGraph:
        """
        This function selects for an interval of gauges. Any component intersecting with the interval will be displayed,
        otherwise deleted.

        :param nx.DiGraph joined_graph: graph
        :param str start_station: first gauge of the interval as a string
        :param str end_station: last gauge of the interval as a string
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return nx.DiGraph: graph that contains only components that intersect with the interval
        """

        comps = list(nx.weakly_connected_components(joined_graph))
        edges = joined_graph.edges()
        comps_copy = comps.copy()
        edges = list(edges)

        gauges = SelectionHandler.get_gauges(comps=comps)

        filtered_stations = sorted_stations[sorted_stations.index(start_station):sorted_stations.index(end_station) + 1]

        final_stations = list(set(gauges) & set(filtered_stations))

        comps_new = []
        for comp in comps_copy:
            list_of_bools = []
            for station in final_stations:
                x = SelectionHandler.is_gauge_in_comp(gauge=station, comp_list=list(comp))
                list_of_bools.append(x)

            if any(list_of_bools):
                comps_new.append(comp)

        nodes_filtered, edges_filtered = SelectionHandler.nodes_and_edges(comps=comps_new, edges=edges)

        g = nx.DiGraph()
        g.add_nodes_from(nodes_filtered)
        g.add_edges_from(edges_filtered)

        return g

    @staticmethod
    def select_by_water_level(joined_graph: nx.DiGraph,
                              station: str,
                              positions: dict,
                              node_colors: list,
                              height: str) -> nx.DiGraph:
        """
        This function selects weakly connected components that have high water level at the given gauge.

        :param nx.DiGraph joined_graph: graph
        :param str station: gauge number to be selected as a string
        :param positions: positions of the graph
        :param node_colors: colors of the nodes of the graph; yellow if the water level is low, red if it's high
        :param str height: level group; either "high" or "low"
        :return nx.DiGraph: graph that contains only components that have high water level at the given gauge
        """

        if height == "high":
            c = "red"
        elif height == "low":
            c = "yellow"
        else:
            raise Exception("Height can be either high or low.")

        fwe = FloodWaveExtractor(joined_graph=joined_graph)
        fwe.get_flood_waves()
        start_end = fwe.flood_waves

        waves = []
        for x, y in start_end:
            waves.append(nx.shortest_path(G=joined_graph, source=x, target=y))

        edges = joined_graph.edges()
        waves_copy = waves.copy()
        edges = list(edges)
        for wave in waves_copy:
            i_zero_list = []
            gauge_in_comp = []
            for i in wave:
                i_zero_list.append(i[0])
                if station == i[0]:
                    gauge_in_comp.append(i)

            if not any(station == elem for elem in i_zero_list):
                waves.remove(wave)
            else:
                colors_of_gauge = []
                for elem in gauge_in_comp:
                    idx = list(positions.keys()).index(elem)
                    colors_of_gauge.append(node_colors[idx])

                if not any(c == color for color in colors_of_gauge):
                    waves.remove(wave)

        nodes_filtered, edges_filtered = SelectionHandler.nodes_and_edges(comps=waves, edges=edges)

        g = nx.DiGraph()
        g.add_nodes_from(nodes_filtered)
        g.add_edges_from(edges_filtered)

        return g

    @staticmethod
    def select_components_from_start_to_end(joined_graph: nx.DiGraph,
                                            start_station: str,
                                            end_station: str,
                                            sorted_stations: list) -> nx.DiGraph:
        """
        This function selects those components that have nodes at both start_station and end_station
        :param nx.DiGraph joined_graph: the graph
        :param str start_station: the first station in the interval
        :param str end_station: the last station in the interval
        :param list sorted_stations: list of strings all station numbers in (numerically) decreasing order
        :return nx.DiGraph: the selected graph
        """
        select_all_in_interval = Selection.select_only_in_interval(joined_graph=joined_graph,
                                                                   start_station=start_station,
                                                                   end_station=end_station,
                                                                   sorted_stations=sorted_stations)

        components = nx.weakly_connected_components(select_all_in_interval)

        selected = []
        for comp in components:
            stations = []
            for node in comp:
                stations.append(node[0])

            if any(start_station == elem for elem in stations) and any(end_station == elem for elem in stations):
                selected.append(comp)

        nodes = []
        for comp in selected:
            nodes += comp

        selected_graph = joined_graph.subgraph(nodes=nodes)

        return selected_graph
