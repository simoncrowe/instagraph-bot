import logging
from os import path
from pathlib import Path


import click
import yaml

from ig_bot.graph import CENTRALITY_METRIC_FUNCTIONS, EIGENVECTOR_CENTRALITY
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
    type=click.Choice(CENTRALITY_METRIC_FUNCTIONS.keys(), case_sensitive=False),
    help='The measure determining the importance of an account.'
)
def scrape_following_graph(data_dir, username, log_level, centrality_metric):
    scrape_graph(data_dir, username, log_level, centrality_metric)


def _load_graph(data_dir: str, logger: logging.Logger):
    graph_path = path.join(data_dir, 'graph.gml')
    try:
        return load_graph_gml(graph_path, logger)
    except OSError:
        return None


def _load_accounts(data_dir: str, logger: logging.Logger):
    accounts_path = path.join(data_dir, 'accounts.csv')
    try:
        return load_dataframe_csv(accounts_path, logger)
    except OSError:
        return None


def _get_logger(data_dir, log_level: str) -> logging.Logger:
    return initialise_logger(data_dir,
                             'log',
                             'ig_bot.scripts.scrape_following_graph',
                             log_level)


def _load_config(config_path):
    with open(config_path, 'r') as fileobj:
        return taml.safe_load(fileobj)


def scrape_graph(data_dir: str,
                 username: str = None,
                 centrality_metric: str = EIGENVECTOR_CENTRALITY,
                 config_path: str = 'config.yaml',
                 log_level: str = 'INFO'):

    # Create data directory if absent
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    config = _load_config(config_path)
    logger = _get_logger(data_dir, log_level)

    graph = _load_graph(data_dir, logger)
    accounts = _load_accounts(data_dir, logger)

    data_present = bool(graph) and bool(accounts)

    if data_present == bool(username):
        raise ValueError(
            'Either a data directory with existing data must be '
            'provided or a username, not both.'
        )


