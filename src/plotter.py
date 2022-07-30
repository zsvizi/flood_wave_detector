from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd

from src.flood_wave_handler import FloodWaveHandler
from src.json_helper import JsonHelper


class Plotter:
    def __init__(self, handler: FloodWaveHandler) -> None:
        self.handler = handler

    def merge_graphs(self,
                     joined_graph: nx.Graph,
                     start_date: str,
                     span: bool,
                     hs: int,
                     he: int,
                     vs: int,
                     ve: int,
                     save: bool = False
                     ) -> None:

        if save:
            Plotter.save_merge_graph(joined_graph=joined_graph)

        start = datetime.strptime(start_date, '%Y-%m-%d')

        positions = self.create_positions(joined_graph=joined_graph, start=start)

        fig, ax = plt.subplots()
        if span:
            ax.axhspan(vs, ve, color='green', alpha=0.3, label="Window for maximum")
            ax.axvspan(hs, he, color='orange', alpha=0.3, label="Window for maximum")

        Plotter.set_x_axis_ticks(
            ax=ax,
            positions=positions,
            start=start,
            rotation=20,
            horizontalalignment='right',
            fontsize=102
        )

        self.set_y_axis_ticks(
            ax=ax,
            rotation=20,
            horizontalalignment='right',
            fontsize=32
        )

        Plotter.format_figure(
            ax=ax,
            xsize=40,
            ysize=10,
            joined_graph=joined_graph,
            positions=positions,
            node_size=1000
        )

        plt.savefig('graph_.pdf')

    def plot_graph(self,
                   start_date: str,
                   end_date: str,
                   save: bool = False
                   ) -> None:

        gauge_peak_plateau_pairs = JsonHelper.read(
                filepath='./saved/find_edges/gauge_peak_plateau_pairs.json',
                log=False
            )

        self.handler.gauge_pairs = list(gauge_peak_plateau_pairs.keys())

        joined_graph = nx.DiGraph()
        for gauge_pair in self.handler.gauge_pairs:
            joined_graph = self.handler.compose_graph(
                end_date=end_date,
                gauge_pair=gauge_pair,
                joined_graph=joined_graph,
                start_date=start_date
            )

        if save:
            Plotter.save_plot_graph(joined_graph)

        start = datetime.strptime(start_date, '%Y-%m-%d')

        positions = self.create_positions(joined_graph=joined_graph, start=start)

        fig, ax = plt.subplots()
        ax.axhspan(4, 9, color='green', alpha=0.3, label="Window for maximum")

        Plotter.set_x_axis_ticks(
            ax=ax,
            positions=positions,
            start=start,
            rotation=20,
            horizontalalignment='right',
            fontsize=15
        )

        self.set_y_axis_ticks(
            ax=ax,
            rotation=20,
            horizontalalignment='right',
            fontsize=22
        )

        Plotter.format_figure(
            ax=ax,
            xsize=30,
            ysize=20,
            joined_graph=joined_graph,
            positions=positions,
            node_size=500
        )

        plt.savefig('graph.pdf')

    @staticmethod
    def save_merge_graph(joined_graph: nx.Graph) -> None:
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(
            filepath=f'./saved/merge_graphs.json',
            obj=joined_graph_save,
            log=False
        )

    @staticmethod
    def save_plot_graph(joined_graph: nx.Graph) -> None:
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(
            filepath=f'./saved/plot_graph.json',
            obj=joined_graph_save,
            log=False
        )

    @staticmethod
    def set_x_axis_ticks(
            ax: plt.axis,
            positions: dict,
            start: datetime,
            rotation: int,
            horizontalalignment: str,
            fontsize: int
    ) -> None:

        min_x = -1
        max_x = max([n[0] for n in positions.values()])
        x_labels = pd.date_range(start - timedelta(days=1),
                                 start + timedelta(days=max_x + 1),
                                 freq='d').strftime('%Y-%m-%d').tolist()
        ax.xaxis.set_ticks(np.arange(min_x - 1, max_x + 1, 1))
        ax.set_xticklabels(
            x_labels,
            rotation=rotation,
            horizontalalignment=horizontalalignment,
            fontsize=fontsize
        )

    def set_y_axis_ticks(self,
                         ax: plt.axis,
                         rotation: int,
                         horizontalalignment: str,
                         fontsize: int
                         ) -> None:

        min_y = 1
        max_y = len(self.handler.gauges) + 1
        y_labels = [str(gauge) for gauge in self.handler.gauges[::-1]]
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(
            y_labels,
            rotation=rotation,
            horizontalalignment=horizontalalignment,
            fontsize=fontsize
        )

    @staticmethod
    def format_figure(
            ax: plt.axis,
            xsize: int,
            ysize: int,
            joined_graph: nx.Graph,
            positions: dict,
            node_size: int
    ) -> None:

        plt.rcParams["figure.figsize"] = (xsize, ysize)
        nx.draw(joined_graph, pos=positions, node_size=node_size)
        plt.axis('on')  # turns on axis
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)

    def create_positions(self,
                         joined_graph: nx.Graph,
                         start: datetime.strptime
                         ) -> dict:

        positions = dict()
        for node in joined_graph.nodes():
            x_coord = abs((start - datetime.strptime(node[1], '%Y-%m-%d')).days) - 1
            y_coord = len(self.handler.gauges) - self.handler.gauges.index(int(node[0]))
            positions[node] = (x_coord, y_coord)
        return positions
