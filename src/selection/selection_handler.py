class SelectionHandler:
    """
    This is a helper class for Selection
    """

    @staticmethod
    def get_gauges(comps: list) -> list:
        """
        This function collects the gauges corresponding to a node in the actual graph
        :param list comps: list of components in the graph
        :return list: decreasingly sorted gauge numbers
        """

        for i in range(len(comps)):
            comps[i] = list(comps[i])
        nodes = [item for sublist in comps for item in sublist]

        gauges = []
        for node in nodes:
            if node[0] not in gauges:
                gauges.append(node[0])
        decreasing_gauges = sorted(gauges, key=lambda x: float(x), reverse=True)

        return decreasing_gauges

    @staticmethod
    def remove_nodes(comps: list, gauges_to_delete: list) -> list:
        """
        This function removes nodes corresponding to gauges_to_delete from all components.

        :param list comps: list of components
        :param list gauges_to_delete: list of gauges to delete
        :return list: remaining components
        """

        comps_copy = comps.copy()
        for comp in comps_copy:
            comp_copy = comp.copy()
            for elem in comp_copy:
                if any(gtd == elem[0] for gtd in gauges_to_delete):
                    comps[comps.index(comp)].remove(elem)
        return comps

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
    def nodes_and_edges(comps: list, edges: list):
        """
        This function finds and collects the nodes and edges of the filtered graph.

        :param list comps: list of the components
        :param list edges: list of the edges
        :return: list of nodes and list of edges
        """

        for i in range(len(comps)):
            comps[i] = list(comps[i])
        nodes = [item for sublist in comps for item in sublist]

        edges_to_keep = []
        for edge in edges:
            found_in_comp = False
            for comp in comps:
                if edge[0] in comp:
                    found_in_comp = True
                    break
            if found_in_comp:
                edges_to_keep.append(edge)

        return nodes, edges_to_keep
