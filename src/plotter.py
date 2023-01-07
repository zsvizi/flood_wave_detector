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
        """
        Constructor for Plotter class

        :param Union[list, None] gauges: The gauges used for the plot.
        """
        self.data = FloodWaveData()
        if gauges is not None:
            self.gauges = gauges
        else:
            self.gauges = self.data.gauges
        self.meta = self.data.meta.loc[self.gauges]

    def plot_graph(self,
                   directed_graph: nx.DiGraph,
                   start_date: str,
                   end_date: str,
                   folder_name: str,
                   save: bool = False,
                   show_nan: bool = False,
                   add_isolated_nodes: bool = True
                   ) -> None:
        """
        Plots a given graph with a given starting date and saves out the plot. If desired it saves the graph as well

        :param nx.DiGraph directed_graph: A graph to be plotted
        :param str end_date: end date for the plotting
        :param str start_date: start date for the figure
        :param str folder_name: Name of the folder to use for file handling.
        :param bool save: Boolean whether to save the graph or not
        :param bool show_nan: flag for showing missing values in the data (thus intervals)
        :param bool add_isolated_nodes: flag for adding the nodes of 0 degree to the graph.
        :return:
        """
        if add_isolated_nodes:
            for gauge in self.gauges:
                nodes = JsonHelper.read(os.path.join(PROJECT_PATH, folder_name, 'find_vertices', str(gauge)+'.json'),
                                        log=False)
                node_lst = []
                for node in nodes:
                    if start_date <= node[0] <= end_date:
                        node_lst.append((str(gauge), node[0]))
                directed_graph.add_nodes_from(node_lst)

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

        Plotter.set_x_axis_ticks(
            ax=ax,
            positions=positions,
            start=min_date,
            rotation=45,
            horizontal_alignment='right',
            fontsize=19
        )

        self.set_y_axis_ticks(
            ax=ax,
            rotation=20,
            horizontal_alignment='right',
            fontsize=19
        )
        
        if show_nan:
            nan_graph = self.create_nan_graph(min_date=str(min_date), max_date=str(max_date))
            nan_positions = FloodWaveHandler.create_positions(
                joined_graph=nan_graph, start=start,
                gauges=self.gauges)
        else:
            nan_graph = None
            nan_positions = None
            
        self.format_figure(
            ax=ax,
            x_size=30,
            y_size=20,
            joined_graph=directed_graph,
            positions=positions,
            node_size=500,
            nan_graph=nan_graph,
            nan_positions=nan_positions)

        plt.savefig(os.path.join(PROJECT_PATH, folder_name, 'graph.png'), bbox_inches='tight')

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
            horizontal_alignment: str,
            fontsize: int
    ) -> None:
        """
        Creating and setting the ticks and labels for the x-axis

        :param plt.axis ax: A figure to set the x-axis ticks and labels for
        :param dict positions: Coordinates for the vertices
        :param datetime start: Start date of the figure
        :param int rotation: Degree of rotation of the labels
        :param str horizontal_alignment: Keyword for alignment
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
            horizontalalignment=horizontal_alignment,
            fontsize=fontsize
        )
        
        ax.set_xticks(ax.get_xticks()[::5])

    def set_y_axis_ticks(self,
                         ax: plt.axis,
                         rotation: int,
                         horizontal_alignment: str,
                         fontsize: int
                         ) -> None:
        """
        Creating and setting the ticks and labels for the y-axis

        :param plt.axis ax: A figure to set the y-axis ticks and labels for
        :param int rotation: Degree of rotation of the labels
        :param str horizontal_alignment: Keyword for alignment
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
            horizontalalignment=horizontal_alignment,
            fontsize=fontsize
        )

    def format_figure(self,
                      ax: plt.axis,
                      x_size: int,
                      y_size: int,
                      joined_graph: nx.DiGraph,
                      positions: dict,
                      node_size: int,
                      nan_graph: nx.DiGraph = None,
                      nan_positions: dict = None
                      ) -> None:
        """
        Formats figure as desired

        :param plt.axis ax: A figure to format
        :param int x_size: Vertical size
        :param int y_size: Horizontal size
        :param nx.Graph joined_graph: A graph to plot
        :param dict positions: Coordinates for the graph
        :param int node_size: Size of the vertices
        :param nx.DiGraph nan_graph: The nan graph to be added to the plot. (empty graph if None is given)
        :param dict nan_positions: The positions for the nan graph's nodes'.
        :return:
        """

        plt.rcParams["figure.figsize"] = (x_size, y_size)
        plt.rcParams.update({
            "savefig.facecolor": (0.0, 0.0, 1.0, 0.0)
        })
        
        nx.draw(joined_graph, pos=positions, node_size=node_size, arrowsize=15, width=2.0) 
        # nx.draw_networkx_labels(joined_graph, pos=positions, labels={n: n[1] for n in joined_graph})
        if nan_graph is not None:
            nx.draw(nan_graph, pos=nan_positions, node_size=200, node_color='red', alpha=0.3)
        plt.axis('on')  # turns on axis
        plt.grid(visible=True, which='major')
        ax.tick_params(left=True, bottom=True, labelleft=True, labelbottom=True)
        ax.set_ylabel('River kilometre', fontsize=30)
        ax.set_xlabel('Date', fontsize=30)
       
        self.create_legend()
        
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
        """
        Formats figure as desired

        :param str min_date: The first date desired to be covered by the nan graph
        :param str max_date: The last date desired to be covered by the nan graph
        :return nx.DiGraph: The nan graph of the given interval
        """
        gauge_data = self.data.dataloader.get_daily_time_series(reg_number_list=self.gauges).loc[min_date:max_date]
        nan_graph = nx.DiGraph()
        for gauge in gauge_data.columns:
            nan_dates = gauge_data[str(gauge)].index[gauge_data[str(gauge)].apply(np.isnan)].strftime("%Y-%m-%d")

            for date in nan_dates:
                nan_graph.add_node(node_for_adding=(gauge, date))

        return nan_graph
