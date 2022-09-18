from datetime import datetime, timedelta
from typing import Union
import os

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.flood_wave_data import FloodWaveData
from src.flood_wave_handler import FloodWaveHandler
from src.json_helper import JsonHelper


class Plotter:
    """This class is responsible for our plotting needs.

    All the functions related to creating figures are located here.
    """
    def __init__(self, gauges: Union[list, None] = None) -> None:
        self.data = FloodWaveData()
        if gauges is not None:
            self.gauges = gauges
        else:
            self.gauges = self.data.gauges
        self.meta = self.data.meta.loc[self.gauges]

    def plot_graph(self,
                   directed_graph: nx.DiGraph,
                   start_date: str,
                   folder_name: str,
                   save: bool = False,
                   show_nan: bool = False
                   ) -> None:
        """
        Plots a given graph with a given starting date and saves out the plot. If desired it saves the graph as well

        :param nx.DiGraph directed_graph: A graph to be plotted
        :param str start_date: start date for the figure
        :param str folder_name: Name of the folder to use for file handling.
        :param bool save: Boolean whether to save the graph or not
        :return:
        """

        if save:
            Plotter.save_plot_graph(directed_graph, folder_name=folder_name)

        start = datetime.strptime(start_date, '%Y-%m-%d')
        min_date = min([node[1] for node in directed_graph.nodes()])
        min_date = datetime.strptime(min_date, '%Y-%m-%d')
        max_date = max([node[1] for node in directed_graph.nodes()])
        max_date = datetime.strptime(max_date, '%Y-%m-%d')
        
                
        positions = FloodWaveHandler.create_positions(joined_graph=directed_graph, start=start,
                                                      gauges=self.gauges)       

        fig, ax = plt.subplots()
        # ax.axhspan(4, 9, color='green', alpha=0.3, label="")

        Plotter.set_x_axis_ticks(
            ax=ax,
            positions=positions,
            start=min_date,
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
        if show_nan:
            nan_graph = self.create_nan_graph(min_date=min_date, max_date=max_date)
            nan_positions = FloodWaveHandler.create_positions(joined_graph=nan_graph, start=start,
                                                      gauges=self.gauges)
            self.format_figure(
            ax=ax,
            xsize=30,
            ysize=20,
            joined_graph=directed_graph,
            positions=positions,
            node_size=500,
            nan_graph=nan_graph,
            nan_positions=nan_positions
        )
        else:
            self.format_figure(
                ax=ax,
                xsize=30,
                ysize=20,
                joined_graph=directed_graph,
                positions=positions,
                node_size=500
            )

        plt.savefig(os.path.join(PROJECT_PATH, folder_name, 'graph.pdf'))

    @staticmethod
    def save_plot_graph(joined_graph: nx.DiGraph, folder_name: str) -> None:
        """
        Saving the graph on the figure

        :param nx.DiGraph joined_graph: The graph to be saved
        :param str folder_name: Name of the folder to use for file handling.
        :return:
        """
        joined_graph_save = nx.node_link_data(joined_graph)
        JsonHelper.write(
            filepath=os.path.join(PROJECT_PATH, folder_name, 'plot_graph.json'),
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
        """
        Creating and setting the ticks and labels for the x-axis

        :param plt.axis ax: A figure to set the x-axis ticks and labels for
        :param dict positions: Coordinates for the vertices
        :param datetime start: Start date of the figure
        :param int rotation: Degree of rotation of the labels
        :param str horizontalalignment: Keyword for alignment
        :param int fontsize: Font size value
        :return:
        """

        min_x = min([n[0] for n in positions.values()])
        max_x = max([n[0] for n in positions.values()])
        rng = max_x - min_x
        x_labels = pd.date_range(start,
                                 start + timedelta(days=rng),
                                 freq='d').strftime('%Y-%m-%d').tolist()
        ax.xaxis.set_ticks(np.arange(min_x, max_x + 1, 1))
        ax.set_xticklabels(
            x_labels,
            rotation=rotation,
            horizontalalignment=horizontalalignment,
            fontsize=fontsize
        )
        ax.set_xticks(ax.get_xticks()[::5])

    def set_y_axis_ticks(self,
                         ax: plt.axis,
                         rotation: int,
                         horizontalalignment: str,
                         fontsize: int
                         ) -> None:
        """
        Creating and setting the ticks and labels for the y-axis

        :param plt.axis ax: A figure to set the y-axis ticks and labels for
        :param int rotation: Degree of rotation of the labels
        :param str horizontalalignment: Keyword for alignment
        :param int fontsize: Font size value
        :return:
        """

        min_y = 1
        max_y = len(self.gauges) + 1
        y_labels = self.meta['river_km'][::-1].round(decimals=1)
        ax.yaxis.set_ticks(np.arange(min_y, max_y, 1))
        ax.set_yticklabels(
            y_labels,
            rotation=rotation,
            horizontalalignment=horizontalalignment,
            fontsize=fontsize
        )

    def format_figure(self,
                      ax: plt.axis,
                      xsize: int,
                      ysize: int,
                      joined_graph: nx.Graph,
                      positions: dict,
                      node_size: int,
                      nan_graph: nx.DiGraph = None,
                      nan_positions: dict = None
                      ) -> None:
        """
        Formats figure as desired

        :param plt.axis ax: A figure to format
        :param int xsize: Vertical size
        :param int ysize: Horizontal size
        :param nx.Graph joined_graph: A graph to plot
        :param dict positions: Coordinates for the graph
        :param int node_size: Size of the vertices
        :return:
        """

        plt.rcParams["figure.figsize"] = (xsize, ysize)
        nx.draw(joined_graph, pos=positions, node_size=node_size)
        nx.draw_networkx_labels(joined_graph, pos=positions, labels={n: n[1] for n in joined_graph})
        if nan_graph is not None:
            nx.draw(nan_graph, pos=nan_positions, node_size=200, node_color='red', alpha=0.3)
        plt.axis('on')  # turns on axis
        plt.grid(visible=True)
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)
        
        legend_elements = self.create_legend()
        ax.legend(labels=legend_elements, loc=5, handlelength=0, handleheight=0)
        
    def create_legend(self) -> list:
        """
        This method is responsible for creating legend for the figure which contains information about gauges.

        :return list: The list containing legend for the figure
        """
        legend_elements = list()
        for gauge in self.gauges:
            legend_elements.append(self.meta['river_km'].loc[gauge].round(decimals=1).astype(str) + '-' +
                                   str(gauge) + '-' +
                                   self.meta['station_name'].loc[gauge])
        return legend_elements
    
    def create_nan_graph(self, min_date: str, max_date: str):
        gauge_data = self.data.dataloader.get_daily_time_series(reg_number_list=self.gauges)\
                                                     .loc[min_date:max_date]
        nan_graph = nx.DiGraph()
        for gauge in gauge_data.columns:
            nan_dates = gauge_data[str(gauge)].index[gauge_data[str(gauge)].apply(np.isnan)].strftime("%Y-%m-%d")

            for date in nan_dates:
                nan_graph.add_node(node_for_adding=(gauge, date))

        return nan_graph