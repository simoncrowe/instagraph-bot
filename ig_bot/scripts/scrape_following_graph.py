from dataclasses import asdict
from datetime import datetime
from itertools import islice
import logging
from os import path
from pathlib import Path
from random import randint
from typing import Iterable, Iterator, List

import click
import networkx as nx
from networkx.exception import PowerIterationFailedConvergence
import pandas as pd
import yaml

from igramscraper.instagram import Instagram

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
    account_by_id,
    account_by_username,
    get_authenticated_igramscraper,
    followed_accounts,
    random_sleep,
)
from ig_bot.scripts.util import (
    get_graph_file_path,
    initialise_logger,
    load_dataframe_csv,
    save_dataframe_csv,
    load_graph_gml,
    save_graph_gml,
)


def _load_graph(graph_path: str, logger: logging.Logger):
    try:
        return load_graph_gml(graph_path, logger)
    except OSError:
        return None


def _load_accounts(accounts_path: str, logger: logging.Logger):
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
def scrape_following_graph(
    data_dir,
    username,
    poorest_centrality_rank,
    config_path,
    log_level
):
    # Create data directory if absent
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    accounts_path = path.join(data_dir, 'accounts.csv')
    graph_path = path.join(data_dir, 'graph.gml')

    config = _load_config(config_path)
    logger = _get_logger(data_dir, log_level)

    graph = _load_graph(graph_path, logger)
    accounts_data = _load_accounts(accounts_path, logger)

    data_present = bool(graph) and accounts_data is not None

    if data_present == bool(username):
        raise ValueError(
            'Either a data directory with existing data must be '
            'provided or a username, not both nor neither.'
        )

    ig_client = get_authenticated_igramscraper(**config['ig_auth'])

    if data_present:
        logger.info("Data present in directory. Selecting scraping candidate...")
        account = top_scraping_candidate(accounts_data,
                                         poorest_centrality_rank)
        logger.info(f"Proceeding to scrape account {account.username}.")

    else:
        logger.info("Data not present in directory.")
        account = account_by_username(username,
                                      ig_client,
                                      config=config,
                                      logger=logger)
        accounts_data = accounts_to_dataframe([account])
        graph = nx.DiGraph()
        add_nodes(graph, account)

        logger.info(f"Proceeding to scrape account {username}..")

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

        accounts_data = record_date_scraped(accounts_data, account)

        logger.info("Adding new follows to graph...")
        add_nodes(graph, *followed)
        add_edges(graph, account, followed)
        save_graph_gml(graph, graph_path, logger)

        logger.info(
            "Detemining which highy ranked followed accounts are new..."
        )
        all_accounts = list(accounts_from_graph(graph, logger))
        new_accounts = list(
            novel_accounts(accounts_data,
                           all_accounts,
                           poorest_centrality_rank)
        )
        logger.info(
            f"Adding {len(new_accounts)} relevent followed accounts to CSV."
        )

        accounts_data = add_accounts_to_data(accounts_data, new_accounts)
        accounts_data = update_centrality(accounts_data, all_accounts)
        save_dataframe_csv(accounts_data, accounts_path, logger, index=False)

        account = top_scraping_candidate(accounts_data,
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
    data: pd.DataFrame, account: Account
) -> pd.DataFrame:
    all_accounts = accounts_from_dataframe(data)
    updated_accounts = list(
        _accounts_date_scraped(all_accounts, account.identifier)
    )
    return accounts_to_dataframe(updated_accounts)


def _accounts_date_scraped(
    accounts: Iterable[Account], account_id: str
) -> Iterator[Account]:

    for account in accounts:
        if account.identifier == account_id:
            account_data = asdict(account)
            account_data['date_scraped'] = datetime.now()
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


def novel_accounts(data: pd.DataFrame,
                   new_accounts: List[Account],
                   accounts_retained: int) -> Iterator[Account]:

    accounts_by_centrality = sorted(new_accounts,
                                    key=lambda a: a.centrality,
                                    reverse=True)
    relevant_ids = set(
        account.identifier
        for account in islice(accounts_by_centrality, accounts_retained)
    )
    existing_ids = set(data['identifier'])
    new_account_ids = relevant_ids.difference(existing_ids)

    return (
        account for account in new_accounts
        if account.identifier in new_account_ids
    )


def add_accounts_to_data(data: pd.DataFrame,
                         accounts: List[Account]) -> pd.DataFrame:
    new_data = accounts_to_dataframe(accounts)
    combined_data = pd.concat([data, new_data])
    return combined_data


def update_centrality(data: pd.DataFrame, accounts: List[Account]):
    all_accounts = accounts_from_dataframe(data)
    updated_accounts = list(
        _accounts_with_centrality_from_summaries(all_accounts, accounts)
    )
    updated_data = accounts_to_dataframe(updated_accounts)
    return updated_data.sort_values(by=['centrality'], ascending=False)


def _accounts_with_centrality_from_summaries(
    accounts: List[Account], summaries: List[Account]
):
    accounts_map = {account.identifier: account for account in accounts}
    summaries_map = {summary.identifier: summary for summary in summaries}

    for identifier, account in accounts_map.items():
        summary = summaries_map.get(identifier)

        if summary:
            account_data = asdict(account)
            account_data['centrality'] = summary.centrality
            yield Account(**account_data)

        else:
            yield account


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


def full_accounts_with_centrality(summaries: Iterable[Account],
                                  client: Instagram,
                                  config: dict,
                                  logger: logging.Logger):
    # TODO: Move or delete as unused?
    sleep_between_account_batches = config['sleep']['between_account_batches']
    sleep_between_accounts = config['sleep']['between_accounts']
    min_accounts_per_batch = config['accounts_per_batch']['minimum']
    max_accounts_per_batch = config['accounts_per_batch']['maximum']

    max_scraped_this_batch = randint(min_accounts_per_batch,
                                     max_accounts_per_batch)
    scraped_count = 0

    for summary in summaries:
        logger.info(f"Scraping data for account {summary.username}")

        account_data = asdict(
            account_by_id(summary.identifier, client, config=config, logger=logger)
        )
        account_data['centrality'] = summary.centrality
        yield Account(**account_data)

        scraped_count += 1
        if scraped_count < max_scraped_this_batch:
            random_sleep(**sleep_between_accounts, logger=logger)
        else:
            random_sleep(**sleep_between_account_batches, logger=logger)
            scraped_count = 0
            max_scraped_this_batch = randint(min_accounts_per_batch,
                                             max_accounts_per_batch)


if __name__ == '__main__':
    scrape_following_graph()
