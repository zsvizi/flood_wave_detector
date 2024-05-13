from src.core.graph_builder import GraphBuilder
from src.core.graph_manipulation import GraphManipulation
from src.core.graph_preparation import GraphPreparation
from src.data.flood_wave_data import FloodWaveData


def main():
    fwd_data = FloodWaveData()
    gauges = fwd_data.gauges

    alpha = 0
    beta = 3
    delta = 2

    folder = 'data_' + str(alpha) + '_' + str(beta) + '_' + str(delta)

    backward_dict = dict()
    forward_dict = dict()
    for gauge in gauges:
        backward_dict[gauge] = alpha
        forward_dict[gauge] = beta

    args = {
        "start_date": '1876-01-01',
        "end_date": '2019-12-31',
        "folder_pf": folder,
        "backward_dict": backward_dict,
        "forward_dict": forward_dict,
        "centered_window_radius": delta,
        "gauges": gauges
    }

    graph_preparation = GraphPreparation(**args)
    graph_preparation.run()
    builder = GraphBuilder()
    builder.build_graph(folder_name=graph_preparation.folder_name)

    args_create = {
        "start_date": '1876-01-01',
        "end_date": '2019-12-31',
        "gauge_pairs": builder.gauge_pairs,
        "folder_name": graph_preparation.folder_name
    }
    GraphManipulation.create_directed_graph(**args_create)


if __name__ == "__main__":
    main()
