"""Instagram scraping logic."""

import logging
from typing import Callable, Dict, List

from igramscraper.instagram import Instagram
from igramscraper.model.account import Account
from igramscraper.exception import (
    InstagramException,
    InstagramNotFoundException
)

from time import sleep
from random import random

from model import AccountNode


def get_authenticated_igramscraper(username: str, password: str):
    """Gets an authenticated igramscraper Instagram client instance."""
    client = Instagram()
    client.with_credentials(username, password)
    client.login()
    return client


def random_sleep(minimum: float, maximum: float, logger: logging.Logger):
    duration = round(minimum + (random() * maximum), 2)
    logger.info(f'Sleeping for {duration} seconds...')
    sleep(duration)


def exponential_sleep(exponent: int, config: Dict, logger: logging.Logger):
    duration = (
            config['exponential_sleep_base'] **
            (config['exponential_sleep_offset'] + exponent)
    )
    logger.info(f'Sleeping for {duration} seconds...')
    sleep(duration)


def get_followed_accounts(
        client: Instagram,
        follower: AccountNode,
        config: Dict,
        logger: logging.Logger,
) -> List[Account]:
    """Gets minimal information for accounts followed by a given account."""
    # TODO: consider making exponential retry logic into a decorator
    attempt_number = 1

    while True:
        try:
            logger.info(
                f'Getting up-to-date follower count for {follower.username}...'
            )
            follower = AccountNode.from_igramscraper_account(
                client.get_account(follower.username)
            )

            logger.info(
                f'Getting accounts followed by "{follower.username}"...'
            )
            return client.get_following(
                account_id=follower.identifier,
                count=follower.follows_count,
                page_size=config['scraping']['follows_page_size'],
            )['accounts']

        except InstagramException as exception:

            if '429' in str(exception):
                logger.exception(
                    f'Prevented from getting accounts followed by '
                    f'{follower.username} by rate limiting.'
                )
                exponential_sleep(
                    exponent=attempt_number,
                    config=config,
                    logger=logger
                )
            else:
                logger.exception(
                    f'Failed to get accounts followed by "{follower.username}".'
                )
                return []

        except InstagramNotFoundException:
            logger.exception(
                f'Failed to get accounts followed by "{follower.username}".'
            )
            return []

        attempt_number += 1


def get_nodes_for_accounts(
        client: Instagram,
        accounts: List[Account],
        all_account_nodes: Dict[str, AccountNode],
        config: dict,
        logger: logging.Logger,
) -> List[AccountNode]:
    """Get nodes for accounts with full information."""
    nodes = []

    for account in accounts:
        nodes.append(
            get_node_for_account(
                client=client,
                account=account,
                all_account_nodes=all_account_nodes,
                config=config,
                logger=logger
            )
        )

    return list(filter(None, nodes))


def get_node_for_account(
        client: Instagram,
        account: Account,
        all_account_nodes: Dict[str, AccountNode],
        config: dict,
        logger: logging.Logger,
):
    if account.identifier in all_account_nodes:
        logger.info(f'Retrieved existing data for "{account.username}".')
        account_node = all_account_nodes[account.identifier]

    else:
        logger.info(f'Getting user data for "{account.username}"...')
        attempt_number = 1

        while True:
            try:
                account_node = AccountNode.from_igramscraper_account(
                    client.get_account(account.username)
                )
                all_account_nodes[account_node.identifier] = account_node
                logger.info(f'Node for "{account.username}" created.')
                random_sleep(
                    logger=logger,
                    **config['sleep_ranges']['after_getting_followed_account']
                )

            except InstagramException as exception:

                if '429' in str(exception):
                    logger.exception(
                        f'Prevented from getting data for '
                        f'{account.username} by rate limiting.'
                    )
                    exponential_sleep(
                        exponent=attempt_number,
                        config=config,
                        logger=logger
                    )
                else:
                    logger.exception(
                        f'Failed to get user data for "{account.username}".'
                    )
                    return None

            except InstagramNotFoundException:
                logger.exception(
                    f'Failed to get accounts followed by "{account.username}".'
                )
                return None

            attempt_number += 1

    return account_node
