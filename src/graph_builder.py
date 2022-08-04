from copy import deepcopy
import os
from queue import LifoQueue

import networkx as nx

from src import PROJECT_PATH
from src.json_helper import JsonHelper
from src.measure_time import measure_time


class GraphBuilder:
    def __init__(self) -> None:
        self.vertex_pairs = {}
        self.gauge_pairs = []
        self.tree_g = nx.DiGraph()
        self.path = {}
        self.all_paths = {}
        self.wave_serial_number = 0
        self.branches = LifoQueue()
        self.flood_wave = {}

    @measure_time
    def build_graph(self) -> None:
        """
        Searching for wave "series". For now, starting from the root ('1514-1515').
        Trying to find the same waves in different gauges.
        """

        # Read the gauge_peak_plateau_pairs (super dict)
        self.vertex_pairs = JsonHelper.read(
                filepath=os.path.join(PROJECT_PATH, 'generated', 'find_edges', 'gauge_peak_plateau_pairs.json')
            )

        self.gauge_pairs = list(self.vertex_pairs.keys())

        for gauge_pair in self.gauge_pairs:

            gauge_pair_dates_dict = self.vertex_pairs[gauge_pair]

            os.makedirs(os.path.join(PROJECT_PATH, 'generated', 'build_graph', f'{gauge_pair}'), exist_ok=True)

            # Search waves starting from the root
            for actual_date in gauge_pair_dates_dict.keys():

                self.reset_tree_and_flood_wave()
                # Go over every date with a wave
                for next_date in gauge_pair_dates_dict[actual_date]:
                    # Empty and reset variables
                    next_g_p_idx = self.reset_gauge_pair_index_and_serial_number()

                    self.add_to_graph(actual_date=actual_date,
                                      gauge_pair=gauge_pair,
                                      next_date=next_date)

                    # Search for flood wave
                    self.create_flood_wave(
                        next_gauge_date=next_date,
                        next_idx=next_g_p_idx
                    )

                    # Go over the missed branches, depth search
                    self.depth_first_search()

                    # Save the wave

                    data = nx.readwrite.json_graph.node_link_data(self.tree_g)
                    JsonHelper.write(
                        filepath=os.path.join(PROJECT_PATH, 'generated', 'build_graph', f'{gauge_pair}/{actual_date}'),
                        obj=data
                    )

    def depth_first_search(self) -> None:
        while self.branches.qsize() != 0:
            # Get info from branches (info about the branch)
            new_date, new_g_p_idx, path_key = self.branches.get()
            self.path = self.all_paths[path_key]

            # Go back to the branch
            self.create_flood_wave(
                next_gauge_date=new_date,
                next_idx=new_g_p_idx
            )

    def create_flood_wave(self,
                          next_gauge_date: str,
                          next_idx: int
                          ) -> None:
        """
        Recursive function walking along the paths in the rooted tree representing the flood wave
        We assume that global variable path contains the complete path up to the current state
        i.e. all nodes (=gauges) are stored before the call of create_flood_wave

        :param str next_gauge_date: The next date, we want to find in the next pair's json.
        A date from the list, not the key. Date after the branch
        :param int next_idx: Index of the next gauge pair.
        E.g: index 1 is referring to "1515-1516" if the root is "1514-1515".
        """

        # other variables
        max_index_value = len(self.vertex_pairs.keys()) - 1
        next_gauge_pair = self.gauge_pairs[next_idx]
        current_gauge = next_gauge_pair.split('_')[0]
        next_gauge = next_gauge_pair.split('_')[1]
        next_gauge_pair_date_dict = self.vertex_pairs[next_gauge_pair]

        # See if we continue the wave
        can_path_be_continued = next_gauge_date in next_gauge_pair_date_dict.keys()

        if can_path_be_continued and next_idx < max_index_value:

            # Get new data values
            new_date_value = next_gauge_pair_date_dict[next_gauge_date]
            # the recursion continues with the first date
            new_gauge_date = new_date_value[0]

            # we store the other possible dates for continuation in a LiFoQueue
            if len(new_date_value) > 1:

                # Save the informations about the branches in a LiFoQueue (branches) so we can come back later.
                for k, dat in enumerate(new_date_value[1:]):
                    self.save_info_about_branches(
                        current_gauge=current_gauge,
                        dat=dat,
                        k=k,
                        next_gauge=next_gauge,
                        next_gauge_date=next_gauge_date,
                        next_idx=next_idx
                    )

            # Update the status of our "place" (path)
            self.update_path_status(
                current_gauge=current_gauge,
                new_gauge_date=new_gauge_date,
                next_gauge=next_gauge,
                next_gauge_date=next_gauge_date
            )

            # Keep going, search for the path
            self.create_flood_wave(
                next_gauge_date=new_gauge_date,
                next_idx=next_idx + 1
            )
        else:

            # Update the 'map'. (Add the path to the start date)
            self.flood_wave[f'id{self.wave_serial_number}'] = self.path

            # Make possible to have more paths
            self.wave_serial_number += 1

    def add_to_graph(self,
                     actual_date: str,
                     gauge_pair: str,
                     next_date: str
                     ) -> None:

        self.reset_path()

        root_gauge = gauge_pair.split('_')[0]
        root_gauge_next = gauge_pair.split('_')[1]

        self.tree_g.add_edge(
            u_of_edge=(root_gauge, actual_date),
            v_of_edge=(root_gauge_next, next_date)
        )

        self.add_to_path(
            actual_date=actual_date,
            next_date=next_date,
            root_gauge=root_gauge,
            root_gauge_next=root_gauge_next
        )

    def add_to_path(self,
                    actual_date: str,
                    next_date: str,
                    root_gauge: str,
                    root_gauge_next: str
                    ) -> None:
        self.path[root_gauge] = actual_date
        self.path[root_gauge_next] = next_date

    def reset_gauge_pair_index_and_serial_number(self) -> int:
        next_g_p_idx = 1
        self.wave_serial_number = 0
        return next_g_p_idx

    def reset_path(self) -> None:
        self.path = {}
        self.all_paths = {}

    def reset_tree_and_flood_wave(self) -> None:
        self.tree_g = nx.Graph()
        self.flood_wave = {}

    def save_info_about_branches(self,
                                 current_gauge: str,
                                 dat: str,
                                 k: int,
                                 next_gauge: str,
                                 next_gauge_date: str,
                                 next_idx: int
                                 ) -> None:

        path_partial = deepcopy(self.path)  # copy result up to now
        self.tree_g.add_edge(
            u_of_edge=(current_gauge, next_gauge_date),
            v_of_edge=(next_gauge, dat)
        )
        path_partial[next_gauge] = dat  # update with the new node and the corresponding possible date
        new_path_key = "path" + str(next_idx + 1) + str(k)
        self.all_paths[new_path_key] = path_partial
        self.branches.put([dat, next_idx + 1, new_path_key])

    def update_path_status(self,
                           current_gauge: str,
                           new_gauge_date: str,
                           next_gauge: str,
                           next_gauge_date: str
                           ) -> None:

        self.tree_g.add_edge(
            u_of_edge=(current_gauge, next_gauge_date),
            v_of_edge=(next_gauge, new_gauge_date)
        )
        self.path[next_gauge] = new_gauge_date
