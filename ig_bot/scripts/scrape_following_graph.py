import csv
from dataclasses import asdict, fields
from datetime import datetime
from itertools import chain, islice
import logging
from operator import attrgetter
from os import path
from pathlib import Path
from random import randint
from typing import Iterable, Iterator, List, Union

import click
import networkx as nx
from networkx.exception import PowerIterationFailedConvergence
import yaml

from ig_bot.data import (
    Account,
    accounts_from_dataframe,
    accounts_to_dataframe,
)
from ig_bot.graph import (
    add_edges,
    add_nodes,
    accounts_with_centrality,
    EIGENVECTOR_CENTRALITY,
    IN_DEGREE_CENTRALITY,
)
from ig_bot.scraping import (
    account_by_username,
    get_authenticated_igramscraper,
    followed_accounts,
    random_sleep,
)
from ig_bot.scripts.util import initialise_logger, load_graph_gml, save_graph_gml


def _load_graph(graph_path: str, logger: logging.Logger):
    try:
        return load_graph_gml(graph_path, logger)
    except OSError:
        return None


def _load_accounts(accounts_path: str, logger: logging.Logger) -> List[Account]:
    try:
        with open(accounts_path, 'r') as file_obj:
            reader = csv.DictReader(file_obj, fieldnames=fields(Account))
            return [Account(**row_data) for row_data in reader]

    except OSError:
        return None

def _save_accounts(accounts: Iterable[Account],
                   accounts_path: str,
                   logger: logging.Logger):
    with open(accounts_path, 'w') as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fields(Account))
        writer.writeheader()
        for account in accounts:
            writer.writerow(asdict(account))


def _get_logger(data_dir, log_level: str) -> logging.Logger:
    return initialise_logger(data_dir,
                             'log',
                             'ig_bot.scripts.scrape_following_graph',
                             log_level)


def _load_config(config_path):
    with open(config_path, 'r') as fileobj:
        return yaml.safe_load(fileobj)


@click.command()
@click.argument('data_dir')
@click.option('--username', '-u', type=str, help='Username of root node.')
@click.option(
    '--poorest-centrality-rank',
    '-r',
    type=int,
    help=(
        'The lowest ranked account in terms of centrality from which to '
        'scrape follows. If all accounts at or above this rank have already '
        'been scraped, this script will exit. '
        'As rankings will change over time, this number is a lower bound '
        'rather than a limit of the total number of accounts scraped.'
    )
)
@click.option('--config-path', '-c', type=str, default='./config.yaml')
@click.option('--log-level', '-l', type=str, default='INFO')
def scrape_following_graph_command(
    data_dir,
    username,
    poorest_centrality_rank,
    config_path,
    log_level
):
    scrape_following_graph(data_dir,
                           username,
                           poorest_centrality_rank,
                           config_path,
                           log_level)


def scrape_following_graph(data_dir: str,
                           username: Union[str, None],
                           poorest_centrality_rank: int,
                           config_path: str,
                           log_level: str):

    # Create data directory if absent
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    accounts_path = path.join(data_dir, 'accounts.csv')
    graph_path = path.join(data_dir, 'graph.gml')

    config = _load_config(config_path)
    logger = _get_logger(data_dir, log_level)

    graph = _load_graph(graph_path, logger)
    accounts = _load_accounts(accounts_path, logger)

    data_present = bool(graph) and accounts is not None

    if data_present == bool(username):
        raise ValueError(
            'Either a data directory with existing data must be '
            'provided or a username, not both nor neither.'
        )

    ig_client = get_authenticated_igramscraper(**config['ig_auth'])

    if data_present:
        logger.info(
            "Data present in directory. Looking for scraping candidate..."
        )
        account = top_scraping_candidate(accounts_data,
                                         poorest_centrality_rank)

    else:
        logger.info("Data not present in directory.")
        account = account_by_username(username,
                                      ig_client,
                                      config=config,
                                      logger=logger)
        accounts = [account]
        graph = nx.DiGraph()
        add_nodes(graph, account)

    sleep_between_account_batches = config['sleep']['between_account_batches']
    sleep_between_accounts = config['sleep']['between_accounts']
    min_accounts_per_batch = config['accounts_per_batch']['minimum']
    max_accounts_per_batch = config['accounts_per_batch']['maximum']

    max_scraped_this_batch = randint(min_accounts_per_batch,
                                     max_accounts_per_batch)
    scraped_this_batch = 0

    while account:
        logger.info(
            f"Scraping accounts followed by {account.username}..."
        )
        followed = list(
            followed_accounts(account, ig_client, config=config, logger=logger)
        )
        logger.info(f"Scraped {len(followed)} accounts.")

        logger.info("Adding new follows to graph...")
        add_nodes(graph, *followed)
        add_edges(graph, account, followed)
        save_graph_gml(graph, graph_path, logger)

        logger.info(
            "Detemining which highy ranked followed accounts are new..."
        )
        all_accounts = list(accounts_from_graph(graph, logger))
        accounts_to_add = list(
            relevant_new_accounts(accounts, all_accounts, poorest_centrality_rank)
        )

        logger.info(
            f"Adding {len(accounts_to_add)} relevent followed accounts to CSV."
        )
        accounts_updated = record_date_scraped(accounts, account)
        relevant_accounts = chain(accounts_updated, accounts_to_add)
        accounts = update_centrality(relevant_accounts, all_accounts)
        _save_accounts(accounts, accounts_path, logger)

        account = top_scraping_candidate(accounts,
                                         poorest_centrality_rank)

        scraped_this_batch += 1
        if scraped_this_batch < max_scraped_this_batch:
            random_sleep(**sleep_between_accounts, logger=logger)
        else:
            random_sleep(**sleep_between_account_batches, logger=logger)
            scraped_this_batch = 0
            max_scraped_this_batch = randint(min_accounts_per_batch,
                                             max_accounts_per_batch)

    logger.info("All relevantly high ranking accounts scraped. Exiting.")


def record_date_scraped(
    all_accounts: Iterable[Account], scraped_account: Account
) -> Iterator[Account]:

    for account in all_accounts:
        if account.identifier == scraped_account.identifier:
            account_data = asdict(account)
            account_data['date_scraped'] = datetime.utcnow()
            yield Account(**account_data)
        else:
            yield account


def accounts_from_graph(graph: nx.DiGraph,
                        logger: logging.Logger) -> Iterator[Account]:
    try:
        yield from accounts_with_centrality(graph, EIGENVECTOR_CENTRALITY)

    except PowerIterationFailedConvergence:
        logger.warning(
            "Convergence failed for eigenvector centrality algorithm. "
            "Falling back on in-degree algorithm."
        )
        yield from accounts_with_centrality(graph, IN_DEGREE_CENTRALITY)


def relevant_new_accounts(existing_accounts: List[Account],
                              all_accounts: List[Account],
                              accounts_retained: int) -> List[Account]:

    accounts_by_centrality = sorted(all_accounts,
                                    key=lambda a: a.centrality,
                                    reverse=True)
    relevant_ids = set(
        account.identifier
        for account in islice(accounts_by_centrality, accounts_retained)
    )
    existing_ids = set(account.identifier for account in existing_accounts)
    new_account_ids = relevant_ids.difference(existing_ids)

    return (
        account for account in all_accounts
        if account.identifier in new_account_ids
    )


def update_centrality(existing_accounts: Iterable[Account],
                      new_accounts: Iterable[Account]) -> List[Account]:

    updated_accounts = _centrality_from_new(existing_accounts, new_accounts)
    return sorted(updated_accounts, key=attrgetter('centrality'), reverse=True)


def _centrality_from_new(old: Iterable[Account],
                         new: Iterable[Account]) -> Iterator[Account]:
    old_accounts_map = {account.identifier: account for account in old}
    new_accounts_map = {account.identifier: account for account in new}

    for identifier, old_account in old_accounts_map.items():
        new_account = new_accounts_map.get(identifier)

        if new_account:
            account_data = asdict(old_account)
            account_data['centrality'] = new_account.centrality
            yield Account(**account_data)

        else:
            yield old_account


def top_scraping_candidate(accounts: Iterable[Account],
                           total_scraped: int) -> Account:
    sorted_accounts = sorted(accounts,
                             key=attrgetter('centrality'),
                             reverse=True)
    top_accounts = islice(sorted_accounts, total_scraped)
    try:
        return next(
            account for account in top_accounts
            if account.date_scraped is None
        )
    except StopIteration:
        return None

if __name__ == '__main__':
    scrape_following_graph_command()
