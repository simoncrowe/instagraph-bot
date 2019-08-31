
import yaml
import math
from os import path
from typing import List

import click
import networkx as nx
import pandas as pd
from sklearn.cluster import KMeans

from graph import account_nodes_from_graph, IMPORTANCE_MEASURE_FUNCTIONS
from model import AccountNode

from data_acquisition.util import initialise_logger


CLUSTERING_CLASSES = {
    'K_MEANS': KMeans,
}


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
    default=math.inf,
    help='Number of important accounts retained.'
)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def cluster_accounts(
        graph_path: str,
        clusters: int,
        clustering_algorithm: str,
        importance_measure: str,
        accounts_retained: int or float,
        log_level: str
):
    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    base_file_name = f'{path.basename(graph_path)}_clustered'

    logger = initialise_logger(
        directory=config['logs_directory'],
        name=base_file_name,
        level=log_level,
    )

    graph = nx.read_gml(graph_path)
    account_nodes = account_nodes_from_graph(graph, logger)
    node_dicts = [node.to_camelcase_dict() for node in account_nodes]
    node_index = [node['identifier'] for node in node_dicts]
    accounts_data = pd.DataFrame(node_dicts, index=node_index)

    importance = IMPORTANCE_MEASURE_FUNCTIONS[importance_measure](graph)
    importance_series = pd.Series(
        [importance[i] for i in node_index],
        index=node_index
    )
    accounts_data['importance'] = importance_series

    if accounts_retained is not math.inf:
        accounts_data.sort_values(
            by=['importance'],
            inplace=True,
            ascending=False
        )
        accounts_data = accounts_data.iloc[:int(accounts_retained)]

    account_data_params = accounts_data[
        ('followsCount', 'followedByCount', 'importance')
    ]
    clustering_model = CLUSTERING_CLASSES[clustering_algorithm](n_clusters=clusters)
    clustering_model.fit(
        accounts_data
    )

    accounts_data.to_csv('test.csv')


if __name__ == '__main__':
    cluster_accounts()
