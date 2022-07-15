from datetime import datetime, timedelta
import os
from typing import Tuple

import matplotlib.pyplot as plt
import networkx as nx
from networkx.readwrite import json_graph
import numpy as np
import pandas as pd

from src.flood_wave_detector import FloodWaveDetector
from src.json_helper import JsonHelper


class Plotter:
    def __init__(self, fwd: FloodWaveDetector) -> None:
        self.fwd = fwd

    def merge_graphs(self, start_station: int, end_station: int, start_date: str, end_date: str,
                     span: bool, hs: int, he: int, vs: int, ve: int, save: bool = False) -> None:

        joined_graph = self.filter_graph(start_station=start_station, end_station=end_station,
                                         start_date=start_date, end_date=end_date)[0]

        if save:
            self.save_merge_graph(joined_graph=joined_graph)

        start = datetime.strptime(start_date, '%Y-%m-%d')

        positions = self.create_positions(joined_graph=joined_graph, start=start)

        fig, ax = plt.subplots()
        if span:
            ax.axhspan(vs, ve, color='green', alpha=0.3, label="Window for maximum")
            ax.axvspan(hs, he, color='orange', alpha=0.3, label="Window for maximum")

        self.set_x_axis_ticks(ax=ax, positions=positions, start=start, rotation=20,
                              horizontalalignment='right', fontsize=102)

        self.set_y_axis_ticks(ax=ax, rotation=20, horizontalalignment='right', fontsize=32)

        self.format_figure(ax=ax, xsize=40, ysize=10, joined_graph=joined_graph, positions=positions, node_size=1000)

        plt.savefig('graph_.pdf')

    def plot_graph(self, start_date: str, end_date: str, save: bool = False) -> None:

        if self.fwd.gauge_peak_plateau_pairs == {}:
            self.fwd.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/find_edges/gauge_peak_plateau_pairs.json',
                                                                log=False)

        self.fwd.gauge_pairs = list(self.fwd.gauge_peak_plateau_pairs.keys())

        joined_graph = nx.DiGraph()
        for gauge_pair in self.fwd.gauge_pairs:
            joined_graph = self.compose_graph(end_date=end_date, gauge_pair=gauge_pair,
                                              joined_graph=joined_graph, start_date=start_date)

        if save:
            self.save_plot_graph(joined_graph)

        start = datetime.strptime(start_date, '%Y-%m-%d')

        positions = self.create_positions(joined_graph=joined_graph, start=start)

        fig, ax = plt.subplots()
        ax.axhspan(4, 9, color='green', alpha=0.3, label="Window for maximum")

        self.set_x_axis_ticks(ax=ax, positions=positions, start=start, rotation=20,
                              horizontalalignment='right', fontsize=15)

        self.set_y_axis_ticks(ax=ax, rotation=20, horizontalalignment='right', fontsize=22)

        self.format_figure(ax=ax, xsize=30, ysize=20, joined_graph=joined_graph, positions=positions, node_size=500)

        plt.savefig('graph.pdf')

    def filter_graph(self, start_station: int, end_station: int, start_date: str, end_date: str)\
            -> Tuple[nx.Graph, int]:

        if self.fwd.gauge_peak_plateau_pairs == {}:
            self.fwd.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/find_edges/gauge_peak_plateau_pairs.json',
                                                                log=False)

        self.fwd.gauge_pairs = list(self.fwd.gauge_peak_plateau_pairs.keys())
        up_limit = self.fwd.meta.loc[start_station].river_km
        low_limit = self.fwd.meta.loc[end_station].river_km

        # first filter
        start_gauges = self.select_start_gauges(low_limit=low_limit)

        selected_pairs = [x for x in self.fwd.gauge_pairs if int(x.split('_')[0]) in start_gauges]

        joined_graph = nx.Graph()
        for gauge_pair in selected_pairs:
            joined_graph = self.compose_graph(end_date=end_date, gauge_pair=gauge_pair,
                                              joined_graph=joined_graph, start_date=start_date)

        # second filter
        self.remove_nodes_with_improper_km_data(joined_graph=joined_graph, low_limit=low_limit, up_limit=up_limit)

        # third filter
        self.date_filter(joined_graph=joined_graph, start_date=start_date, end_date=end_date)

        # fourth filter
        connected_components = [list(x) for x in nx.connected_components(joined_graph)]

        self.remove_components_not_including_start_or_end_station(connected_components=connected_components,
                                                                  start_station=start_station, end_station=end_station,
                                                                  joined_graph=joined_graph)

        total_waves = self.count_waves(connected_components=connected_components, joined_graph=joined_graph,
                                       start_station=start_station, end_station=end_station)

        return joined_graph, total_waves

    @staticmethod
    def save_merge_graph(joined_graph: nx.Graph) -> None:
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(filepath=f'./saved/merge_graphs.json', obj=joined_graph_save, log=False)

    @staticmethod
    def save_plot_graph(joined_graph: nx.Graph) -> None:
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(filepath=f'./saved/plot_graph.json', obj=joined_graph_save, log=False)

    @staticmethod
    def set_x_axis_ticks(ax: plt.axis, positions: dict, start: datetime, rotation: int, horizontalalignment: str,
                         fontsize: int) -> None:
        min_x = -1
        max_x = max([n[0] for n in positions.values()])
        x_labels = pd.date_range(start - timedelta(days=1),
                                 start + timedelta(days=max_x + 1),
                                 freq='d').strftime('%Y-%m-%d').tolist()
        ax.xaxis.set_ticks(np.arange(min_x - 1, max_x + 1, 1))
        ax.set_xticklabels(x_labels, rotation=rotation, horizontalalignment=horizontalalignment, fontsize=fontsize)

    def set_y_axis_ticks(self, ax: plt.axis, rotation: int, horizontalalignment: str, fontsize: int) -> None:
        min_y = 1
        max_y = len(self.fwd.gauges) + 1
        y_labels = [str(gauge) for gauge in self.fwd.gauges[::-1]]
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(y_labels, rotation=rotation, horizontalalignment=horizontalalignment, fontsize=fontsize)

    @staticmethod
    def format_figure(ax: plt.axis, xsize: int, ysize: int, joined_graph: nx.Graph, positions, node_size: int) -> None:
        plt.rcParams["figure.figsize"] = (xsize, ysize)
        nx.draw(joined_graph, pos=positions, node_size=node_size)
        plt.axis('on')  # turns on axis
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)

    def create_positions(self, joined_graph: nx.Graph, start: datetime.strptime) -> dict:
        positions = dict()
        for node in joined_graph.nodes():
            x_coord = abs((start - datetime.strptime(node[1], '%Y-%m-%d')).days) - 1
            y_coord = len(self.fwd.gauges) - self.fwd.gauges.index(int(node[0]))
            positions[node] = (x_coord, y_coord)
        return positions

    def compose_graph(self, joined_graph: nx.Graph, gauge_pair, start_date: str, end_date: str) -> nx.Graph:
        filenames = next(os.walk(f'./saved/build_graph/{gauge_pair}'), (None, None, []))[2]
        sorted_files = self.fwd.sort_wave(filenames=filenames, start=start_date, end=end_date)
        for file in sorted_files:
            data = JsonHelper.read(filepath=f'./saved/build_graph/{gauge_pair}/{file}', log=False)
            h = json_graph.node_link_graph(data)
            joined_graph = nx.compose(joined_graph, h)
        return joined_graph

    @staticmethod
    def remove_components_not_including_start_or_end_station(connected_components, start_station: int, end_station: int,
                                                             joined_graph: nx.Graph) -> None:
        for sub_connected_component in connected_components:
            res_start = [int(node[0]) == start_station for node in sub_connected_component]
            res_end = [int(node[0]) == end_station for node in sub_connected_component]
            if (True not in res_start) or (True not in res_end):
                joined_graph.remove_nodes_from(sub_connected_component)

    @staticmethod
    def count_waves(connected_components, joined_graph: nx.Graph, start_station: int, end_station: int) -> int:
        total_waves = 0
        for sub_connected_component in connected_components:
            start_nodes = [node for node in sub_connected_component if int(node[0]) == start_station]
            end_nodes = [node for node in sub_connected_component if int(node[0]) == end_station]
            for start in start_nodes:
                for end in end_nodes:
                    paths = [list(x) for x in nx.all_shortest_paths(joined_graph, source=start, target=end)]
                    total_waves += len(paths)
        return total_waves

    @staticmethod
    def date_filter(joined_graph: nx.Graph, end_date: str, start_date: str) -> None:
        remove_date = [x for x in joined_graph.nodes if ((x[1] > end_date) or (x[1] < start_date))]
        joined_graph.remove_nodes_from(remove_date)

    def remove_nodes_with_improper_km_data(self, joined_graph: nx.Graph, low_limit: int, up_limit: int) -> None:
        selected_meta = self.fwd.meta[(self.fwd.meta['river_km'] >= low_limit) &
                                      (self.fwd.meta['river_km'] <= up_limit)]

        comp_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        comp = [x for x in self.fwd.gauges if x not in comp_gauges]
        remove = [x for x in joined_graph.nodes if int(x[0]) in comp]
        joined_graph.remove_nodes_from(remove)

    def select_start_gauges(self, low_limit: int) -> list:
        selected_meta = self.fwd.meta[(self.fwd.meta['river_km'] >= low_limit)]
        start_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        return start_gauges
