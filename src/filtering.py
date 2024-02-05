import networkx as nx


class Filtering:
    """
    This class contains filtering functions for the final graph.
    """

    def filter_by_gauge(self, graph: nx.DiGraph, gauge: str) -> nx.DiGraph:
        """
        This function filters out weakly connected components that have the given gauge as a node.

        :param nx.DiGraph graph: graph to be filtered
        :param str gauge: gauge number to be filtered by as a string
        :return nx.DiGraph: graph that contains only components which have gauge as a node
        """

        comps = list(nx.weakly_connected_components(graph))
        edges = graph.edges()
        comps_copy = comps.copy()
        edges = list(edges)
        edges_copy = edges.copy()

        for comp in comps_copy:
            if not self.is_gauge_in_comp(gauge=gauge, comp_list=list(comp)):
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

    def filter_multiple_gauges(self, graph: nx.DiGraph, start_gauge: str, end_gauge: str) -> nx.DiGraph:
        """
        This function filters for an interval of gauges. Any component starting in the interval will be displayed,
        otherwise deleted.

        :param nx.DiGraph graph: graph to be filtered
        :param str start_gauge: first gauge of the interval as a string
        :param str end_gauge: last gauge of the interval as a string
        :return nx.DiGraph: graph that contains only components which have their starting node in the given interval
        """

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
                x = self.is_gauge_in_comp(gauge=fg, comp_list=list(comp))
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
    def is_gauge_in_comp(gauge: str, comp_list: list) -> bool:
        """
        This function checks whether the weakly connected component comp_list has a node at gauge.

        :param str gauge: given gauge number as a string
        :param list comp_list: given weakly connected component as a list
        :return bool: True if the gauge is in the component, False otherwise
        """

        return any(gauge == elem for elem in [i[0] for i in [list(ele) for ele in comp_list]])

    @staticmethod
    def filter_by_water_level(graph: nx.DiGraph, gauge: str, positions: dict, node_colors: list) -> nx.DiGraph:
        """
        This function filters out weakly connected components that have high water level at the given gauge.

        :param nx.DiGraph graph: graph to be filtered
        :param str gauge: gauge number to be filtered by as a string
        :param positions: positions of the graph
        :param node_colors: colors of the nodes of the graph; yellow if the water level is low, red if it's high
        :return nx.DiGraph: graph that contains only components that have high water level at the given gauge
        """

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
