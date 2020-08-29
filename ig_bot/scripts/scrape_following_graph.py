from os import path

import click
import yaml

from ig_bot.graph import EIGENVECTOR_CENTRALITY
from ig_bot.scripts.util import (
    get_graph_file_path,
    initialise_logger,
    load_dataframe_csv,
    load_graph_gml,
    save_graph_gml,
)


@click.command()
@click.argument('data_dir')
@click.option('--username', '-u', type=str, help='Username of root node.')
@click.option('--log-level', '-l', type=str)
@click.option(
    '--centrality-metric',
    '-c',
    type=str,
    help='The measure determining the importance of an account.'
)
def scrape_following_graph(data_dir, username, log_level, centrality_metric):
    scrape_graph(data_dir, username, log_level, centrality_metric)


def scrape_graph(data_dir: str,
                 username: str = None,
                 log_level: str = 'INFO',
                 centrality_metric: str = EIGENVECTOR_CENTRALITY):

    #with open('config.yaml') as file_obj:
    #    config = yaml.safe_load(file_obj)

    logger = initialise_logger(data_dir,
                               'log',
                               'ig_bot.scripts.scrape_following_graph',
                               log_level)

    graph_path = path.join(data_dir, 'graph.gml')
    try:
        graph = load_graph_gml(graph_path, logger)
    except OSError:
        graph = None

    accounts_path = path.join(data_dir, 'accounts.csv')
    try:
        accounts = load_dataframe_csv(accounts_path, logger)
    except OSError:
        accounts = None

    data_present = graph and accounts

    if bool(data_present) == bool(username):
        raise ValueError(
            'Either a data directory with existing data must be '
            'provided or a username, not both.'
        )


