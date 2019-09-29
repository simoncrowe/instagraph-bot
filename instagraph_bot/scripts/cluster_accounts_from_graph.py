
import yaml
import math
from os import path
from typing import List

import click
import networkx as nx
import numpy as np
import pandas as pd
from sklearn  import cluster
from sklearn import preprocessing

from graph import account_nodes_from_graph, CENTRALITY_METRIC_FUNCTIONS

from scripts.util import initialise_logger, save_dataframe_csv


CLUSTERING_CLASSES = {'K_MEANS': cluster.KMeans}


@click.command()
@click.option(
    '--graph', '-g', 'graph_path', required=True, help='Path to GML file.')
@click.option(
    '--clusters',
    '-c',
    type=int,
    default=4,
    help='Number of clusters.'
)
@click.option(
    '--clustering-algorithm',
    '-a',
    default='K_MEANS',
    help='The algorithm used to cluster the accounts.'
)
@click.option(
    '--importance-measure',
    '-i',
    type=str,
    default='EIGENVECTOR_CENTRALITY',
    help='The measure determining the importance of an account.'
)
@click.option(
    '--accounts-retained',
    '-r',
    type=int,
    default=-1,
    help=(
            'Number of important accounts retained. '
            '(Negative value means all are retained.)'
    )
)
@click.option(
    '--max-followers',
    '-f',
    type=int,
    default=-1,
    help=(
            'The maximum number of followers of retained accounts. '
            'This is intended as a crude means of removing outliers. '
            '(A negative value means that there is no maximum.'
    )
)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def cluster_accounts(
        graph_path: str,
        clusters: int,
        clustering_algorithm: str,
        importance_measure: str,
        accounts_retained: int,
        max_followers: int,
        log_level: str
):
    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    base_file_name = (
        f'{path.splitext(path.basename(graph_path))[0]}_'
        f'{accounts_retained}-clustered-{clustering_algorithm}-{clusters}'
    )

    logger = initialise_logger(
        directory=config['logs_directory'],
        name=base_file_name,
        module='instagraph_bot.scripts.cluster_accounts_from_graph',
        level=log_level,
    )

    graph = nx.read_gml(graph_path)
    account_nodes = account_nodes_from_graph(graph, logger)
    node_dicts = [node.to_camelcase_dict() for node in account_nodes]
    node_index = [node['identifier'] for node in node_dicts]
    accounts_data = pd.DataFrame(node_dicts, index=node_index)

    importance = CENTRALITY_METRIC_FUNCTIONS[importance_measure](graph)
    centrality_series = pd.Series(
        [importance[i] for i in node_index],
        index=node_index
    )
    accounts_data['centrality'] = centrality_series

    # Negative value for max_followers mean that here is not maximum
    if max_followers > -1:
        accounts_data = accounts_data.loc[
            accounts_data['followedByCount'] <= max_followers
        ]

    # Negative value for accounts_retained means that all are retained.
    if accounts_retained > -1:
        accounts_data.sort_values(
            by=['centrality'],
            inplace=True,
            ascending=False
        )
        accounts_data = accounts_data.iloc[:int(accounts_retained)]

    features = accounts_data[
        ['followsCount', 'followedByCount', 'centrality']
    ].to_numpy(dtype=np.float32)

    zero_one_scaler = preprocessing.MinMaxScaler()
    scaled_features = zero_one_scaler.fit_transform(features)

    model = CLUSTERING_CLASSES[clustering_algorithm](n_clusters=clusters)
    model.fit(scaled_features)
    cluster_labels = model.predict(scaled_features)

    cluster_series = pd.Series(cluster_labels, index=accounts_data.index)
    accounts_data['cluster'] = cluster_series

    data_dir = config['data_directory']
    save_dataframe_csv(
        df=accounts_data,
        filepath=path.join(data_dir, f'{base_file_name}.csv'),
        logger=logger,
    )


if __name__ == '__main__':
    cluster_accounts()
