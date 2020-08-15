import functools
import logging
from random import random, randint
import time
from typing import Dict, Generator, List

from igramscraper.instagram import Instagram
from igramscraper.exception import InstagramException

from ig_bot.data import Account, AccountSummary, account_from_obj, account_summary_from_obj


class MaxRateLimitingRetriesExceeded(Exception):
    """The service still rate limits after the maxiumum number of attempts."""


def _get_authenticated_igramscraper(username: str, password: str):
    """Gets an authenticated igramscraper Instagram client instance."""
    client = Instagram()
    client.with_credentials(username, password)
    client.login()
    return client


def _random_sleep(minimum: float, maximum: float, logger: logging.Logger):
    duration = round(minimum + (random() * (maximum - minimum)), 2)
    logger.info(f'Sleeping for {duration} seconds...')
    time.sleep(duration)


def exponential_sleep(exponent: int, base: float, offset: float, logger: logging.Logger):
    duration = (base ** exponent) + offset
    duration_rounded = round(duration, 2)
    logger.info(f'Sleeping for {duration_rounded} seconds...')
    time.sleep(duration_rounded)


def retry_on_rate_limiting(func):

    @functools.wraps(func)
    def wrapper(*args, config, logger, **kwargs):

        retries = config["rate_limit_retries"]
        for attempt_number in range(1, retries + 1):
            try:
                return func(*args, config=config, logger=logger, **kwargs)
            except InstagramException as exception:
                if '429' in str(exception):
                    logger.exception("Scraping failed due to rate limiting")

                    base = config['exponential_sleep_base']
                    offset = config['exponetial_sleep_offset']
                    exponential_sleep(attempt_number, base, offset, logger)

        raise MaxRateLimitingRetriesExceeded(
            f"Function {func.__name__} still failed due to rate limiting "
            f"after {attempt_number} attempts."
        )

    return wrapper


@retry_on_rate_limiting
def followed_accounts(
        follower: Account,
        client: Instagram, 
        config: dict, 
        logger: logging.Logger
) -> Generator[AccountSummary, None, None]:
    response = client.get_following(
        account_id=follower.identifier,
     	count=follower.follows_count,
     	page_size=config['scraping']['follows_page_size'],
    )
    return (account_summary_from_obj(account) for account in response['accounts'])
       

@retry_on_rate_limiting
def account_by_id(identifier: str,
                  client: Instagram,
                  config: dict,
                  logger: logging.Logger) -> Account:

    return account_from_obj(client.get_account_by_id(identifier))


@retry_on_rate_limiting
def account_by_username(username: str,
                        client: Instagram,
                        config: dict,
                        logger: logging.Logger) -> Account:

    return account_from_obj(client.get_account(username))


def get_followed_accounts(
        client: Instagram,
        follower: Account,
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
            #follower = AccountNode.from_igramscraper_account(
            #    client.get_account(follower.username)
            #)
            random_sleep(
                logger=logger,
                **config['sleep_ranges']['after_getting_account_data']
            )
            logger.info(
                f'Getting accounts followed by "{follower.username}"...'
            )

            #accounts_following = client.get_following(
            #    account_id=follower.identifier,
            #    count=follower.follows_count,
            #    page_size=config['scraping']['follows_page_size'],
            #)['accounts']
            random_sleep(
                logger=logger,
                **config['sleep_ranges']['after_getting_followed_accounts']
            )
            return accounts_following

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

        except Exception:
            logger.exception(
                f'Failed to get accounts followed by "{follower.username}".'
            )
            return []

        attempt_number += 1


def get_nodes_for_accounts(
        client: Instagram,
        accounts: List[Account],
        all_account_nodes: Dict[str, object],
        config: dict,
        logger: logging.Logger,
) -> List[object]:
    """Get nodes for accounts with full information."""
    nodes = []
    scraped_count = 0
    max_scraped_this_round = randint(
        config['accounts_scraped_per_round']['minimum'],
        config['accounts_scraped_per_round']['maximum']
    )

    for account in accounts:
        node, scraping_needed = get_node_for_account(
            client=client,
            account=account,
            all_account_nodes=all_account_nodes,
            config=config,
            logger=logger
        )
        if scraping_needed:
            scraped_count += 1

            if scraped_count < max_scraped_this_round:
                random_sleep(
                    **config['sleep_ranges']['after_getting_account_data'],
                    logger=logger
                )
            else:
                random_sleep(
                    **config['sleep_ranges']['between_scraping_rounds'],
                    logger=logger
                )
                scraped_count = 0
                max_scraped_this_round = randint(
                    config['accounts_scraped_per_round']['minimum'],
                    config['accounts_scraped_per_round']['maximum']
                )

        if node:
            nodes.append(node)

    return nodes


def get_node_for_account(
        client: Instagram,
        account: Account,
        all_account_nodes: Dict[str, object],
        config: dict,
        logger: logging.Logger,
):
    if account.identifier in all_account_nodes:
        scraping_needed = False
        logger.info(f'Retrieved existing data for "{account.username}".')
        account_node = all_account_nodes[account.identifier]

    else:
        scraping_needed = True
        logger.info(f'Getting user data for "{account.username}"...')
        attempt_number = 1

        while True:
            try:
                #account_node = AccountNode.from_igramscraper_account(
                #    client.get_account(account.username)
                #)
                all_account_nodes[account_node.identifier] = account_node
                logger.info(f'Node for "{account.username}" created.')
                break

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
                    account_node = None
                    break

            except Exception:
                logger.exception(
                    f'Failed to get accounts followed by "{account.username}".'
                )
                account_node = None
                break

            attempt_number += 1

    return account_node, scraping_needed
