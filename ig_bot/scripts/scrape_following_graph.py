from dataclasses import asdict
from itertools import islice
import logging
from os import path
from pathlib import Path
from typing import Iterable

import click
import pandas as pd
import yaml

from ig_bot.data import Account, AccountSummary, accounts_to_dataframe
from ig_bot.graph import CENTRALITY_METRIC_FUNCTIONS, EIGENVECTOR_CENTRALITY
from ig_bot.scraping import account_by_id
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
    accounts_data = _load_accounts(data_dir, logger)

    data_present = bool(graph) and bool(accounts_data)

    if data_present == bool(username):
        raise ValueError(
            'Either a data directory with existing data must be '
            'provided or a username, not both nor neither.'
        )


def update_accounts_data(data: pd.DataFrame,
                         new_accounts: Iterable[AccountSummary],
                         accounts_retained: int) -> pd.DataFrame:

    accounts_by_centrality = sorted(new_accounts,
                                    key=lambda a: a.centrality,
                                    reverse=True)
    relevant_ids = set(
        account.identifier
        for account in islice(accounts_by_centrality, accounts_retained)
    )
    existing_ids = set(data['identifier'])
    ids_to_scrape = relevant_ids.difference(existing_ids)
    accounts_to_scrape = (
        a for a in new_accounts if a.identifier in ids_to_scrape
    )

    accounts = list(_full_accounts_with_centrality(accounts_to_scrape))
    new_data = accounts_to_dataframe(accounts)

    combined_data = pd.concat([data, new_data])
    combined_sorted = combined_data.sort_values(["centrality"], ascending=False)
    return combined_sorted


def top_scraping_candidate(accounts_data: pd.DataFrame, total_scraped: int) -> Account:
    sorted_data = accounts_data.sort_values(["centrality"], ascending=False)
    top_data = sorted_data.head(total_scraped)
    candidates = top_data[top_data.date_scraped.isnull()]

    try:
        row_data = next(candidates.itertuples(index=False))._asdict()
        # Overwriting Pandas NaT value
        row_data["date_scraped"] = None
        return Account(**row_data)

    except StopIteration:
        return None


def _full_accounts_with_centrality(summaries: Iterable[AccountSummary]):
    for summary in summaries:
        account_data = asdict(account_by_id(summary.identifier))
        account_data['centrality'] = summary.centrality
        yield Account(**account_data)

