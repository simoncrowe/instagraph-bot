"""Instagram scraping logic."""

import logging
from typing import Dict, List

from igramscraper.instagram import Instagram
from igramscraper.model.account import Account

from time import sleep
from random import random

from model import AccountNode


def get_authenticated_igramscraper(username: str, password: str):
    """Gets an authenticated igramscraper Instagram client instance."""
    client = Instagram()
    client.with_credentials(username, password)
    client.login()
    return client


def random_sleep(minimum: float, maximum: float):
    sleep(minimum + (random() * maximum))


def get_followed_accounts(
        client: Instagram,
        follower: AccountNode,
        config: dict,
        logger: logging.Logger,
) -> List[Account]:
    """Gets minimal information for accounts followed by a given account."""
    try:
        logger.info(f'Getting accounts followed by "{follower.username}"...')
        return client.get_following(
            account_id=follower.identifier,
            count=follower.follows_count,
            page_size=config['scraping']['follows_page_size'],
        )['accounts']
    except Exception:
        logger.exception(
            f'Failed to get accounts followed by "{follower.username}".'
        )
        return []


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
        if account.identifier in all_account_nodes:
            nodes.append(all_account_nodes[account.identifier])
            logger.info(f'Retrieved existing data for "{account.username}".')
        else:
            logger.info(f'Getting user data for "{account.username}"...')
            try:
                account_node = AccountNode.from_igramscraper_account(
                    client.get_account(account.username)
                )
                all_account_nodes[account_node.identifier] = account_node
                nodes.append(account_node)
            except Exception:
                logger.exception(
                    f'Failed to get user data for "{account.username}".'
                )

            logger.info(f'Node for "{account.username}" created.')
            random_sleep(**config['sleep']['after_getting_followed_account'])

    return nodes
