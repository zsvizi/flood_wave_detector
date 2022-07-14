from datetime import datetime, timedelta
import os

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
                     span: bool, hs: int, he: int, vs, ve, save: bool):

        joined_graph = self.filter_graph(start_station=start_station, end_station=end_station,
                                         start_date=start_date, end_date=end_date)[0]

        if save:
            self.save_merge_graph(joined_graph=joined_graph)

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        positions = self.create_positions(joined_graph=joined_graph, start=start)

        fig, ax = plt.subplots()
        if span:
            ax.axhspan(vs, ve, color='green', alpha=0.3, label="Window for maximum")
            ax.axvspan(hs, he, color='orange', alpha=0.3, label="Window for maximum")
        x_labels = self.set_ticks_x_label(ax, positions, start)
        ax.set_xticklabels(x_labels, rotation=20, horizontalalignment='right', fontsize=102)

        min_y = 1
        max_y = len(self.fwd.gauges) + 1
        y_labels = [str(gauge) for gauge in self.fwd.gauges[::-1]]
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(y_labels, rotation=20, horizontalalignment='right', fontsize=32)

        plt.rcParams["figure.figsize"] = (40, 10)
        nx.draw(joined_graph, pos=positions, node_size=1000)
        plt.axis('on')  # turns on axis
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)
        plt.savefig('graph_.pdf')

    def plot_graph(self, start_date: str, end_date: str, save: bool):

        if self.fwd.gauge_peak_plateau_pairs == {}:
            self.fwd.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/step2/gauge_peak_plateau_pairs.json',
                                                                log=False)

        self.fwd.gauge_pairs = list(self.fwd.gauge_peak_plateau_pairs.keys())

        joined_graph = nx.DiGraph()
        for gauge_pair in self.fwd.gauge_pairs:
            joined_graph = self.compose_graph(end_date=end_date, gauge_pair=gauge_pair,
                                              joined_graph=joined_graph, start_date=start_date)

        if save:
            self.save_plot_graph(joined_graph)

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        positions = self.create_positions(joined_graph, start)

        fig, ax = plt.subplots()
        ax.axhspan(4, 9, color='green', alpha=0.3, label="Window for maximum")
        x_labels = self.set_ticks_x_label(ax, positions, start)
        ax.set_xticklabels(x_labels, rotation=20, horizontalalignment='right', fontsize=15)

        min_y = 1
        max_y = len(self.fwd.gauges) + 1
        y_labels = [str(gauge) for gauge in self.fwd.gauges[::-1]]
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(y_labels, rotation=20, horizontalalignment='right', fontsize=22)

        plt.rcParams["figure.figsize"] = (30, 20)
        nx.draw(joined_graph, pos=positions, node_size=500)
        limits = plt.axis('on')  # turns on axis
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)
        plt.savefig('graph.pdf')

    def filter_graph(self, start_station: int, end_station: int, start_date: str, end_date: str):

        if self.fwd.gauge_peak_plateau_pairs == {}:
            self.fwd.gauge_peak_plateau_pairs = JsonHelper.read(filepath='./saved/step2/gauge_peak_plateau_pairs.json',
                                                                log=False)

        # first filter
        self.fwd.gauge_pairs = list(self.fwd.gauge_peak_plateau_pairs.keys())
        up_limit = self.fwd.meta.loc[start_station].river_km
        low_limit = self.fwd.meta.loc[end_station].river_km
        selected_meta = self.fwd.meta[(self.fwd.meta['river_km'] >= low_limit)]
        start_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()

        selected_pairs = [x for x in self.fwd.gauge_pairs if int(x.split('_')[0]) in start_gauges]

        joined_graph = nx.Graph()
        for gauge_pair in selected_pairs:
            joined_graph = self.compose_graph(end_date=end_date, gauge_pair=gauge_pair,
                                              joined_graph=joined_graph, start_date=start_date)

        selected_meta = self.fwd.meta[(self.fwd.meta['river_km'] >= low_limit) &
                                      (self.fwd.meta['river_km'] <= up_limit)]
        comp_gauges = selected_meta.dropna(subset=['h_table']).index.tolist()
        # second filter
        comp = [x for x in self.fwd.gauges if x not in comp_gauges]
        remove = [x for x in joined_graph.nodes if int(x[0]) in comp]
        print(joined_graph.nodes)
        joined_graph.remove_nodes_from(remove)
        print(joined_graph.nodes)

        # third filter
        remove_date = [x for x in joined_graph.nodes if ((x[1] > end_date) or (x[1] < start_date))]
        joined_graph.remove_nodes_from(remove_date)
        print(joined_graph.nodes)

        # fourth filter
        cc = [list(x) for x in nx.connected_components(joined_graph)]
        for sub_cc in cc:
            res_start = [int(node[0]) == start_station for node in sub_cc]
            res_end = [int(node[0]) == end_station for node in sub_cc]
            if (True not in res_start) or (True not in res_end):
                joined_graph.remove_nodes_from(sub_cc)
        total_waves = 0
        for sub_cc in cc:
            start_nodes = [node for node in sub_cc if int(node[0]) == start_station]
            end_nodes = [node for node in sub_cc if int(node[0]) == end_station]
            for start in start_nodes:
                for end in end_nodes:
                    paths = [list(x) for x in nx.all_shortest_paths(joined_graph, source=start, target=end)]
                    total_waves += len(paths)

        return joined_graph, total_waves

    @staticmethod
    def save_merge_graph(joined_graph: nx.Graph):
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(filepath=f'./saved/merge_graphs.json', obj=joined_graph_save, log=False)

    @staticmethod
    def save_plot_graph(joined_graph: nx.Graph):
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(filepath=f'./saved/plot_graph.json', obj=joined_graph_save, log=False)

    @staticmethod
    def set_ticks_x_label(ax, positions, start):
        min_x = -1
        max_x = max([n[0] for n in positions.values()])
        x_labels = pd.date_range(start - timedelta(days=1),
                                 start + timedelta(days=max_x + 1),
                                 freq='d').strftime('%Y-%m-%d').tolist()
        ax.xaxis.set_ticks(np.arange(min_x - 1, max_x + 1, 1))
        return x_labels

    def create_positions(self, joined_graph: nx.Graph, start):
        positions = dict()
        for node in joined_graph.nodes():
            x_coord = abs((start - datetime.strptime(node[1], '%Y-%m-%d')).days) - 1
            y_coord = len(self.fwd.gauges) - self.fwd.gauges.index(int(node[0]))
            positions[node] = (x_coord, y_coord)
        return positions

    def compose_graph(self, end_date, gauge_pair, joined_graph, start_date):
        filenames = next(os.walk(f'./saved/step3/{gauge_pair}'), (None, None, []))[2]
        sorted_files = self.fwd.sort_wave(filenames=filenames, start=start_date, end=end_date)
        for file in sorted_files:
            data = JsonHelper.read(filepath=f'./saved/step3/{gauge_pair}/{file}', log=False)
            h = json_graph.node_link_graph(data)
            # Read a file and load it
            joined_graph = nx.compose(joined_graph, h)
        return joined_graph
