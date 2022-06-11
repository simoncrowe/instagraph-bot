import functools
import logging
from random import random
import time
from typing import Generator

from igramscraper.instagram import Instagram
from igramscraper.exception import InstagramException

from ig_bot.data import Account, account_from_obj


class MaxRateLimitingRetriesExceeded(Exception):
    """The service still rate limits after the maxiumum number of attempts."""


def get_authenticated_igramscraper(username: str, password: str):
    """Gets an authenticated igramscraper Instagram client instance."""
    client = Instagram()
    client.with_credentials(username, password)
    client.login(two_step_verificator=True)
    return client


def random_sleep(minimum: float, maximum: float, logger: logging.Logger):
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
                    offset = config['exponential_sleep_offset']
                    exponential_sleep(attempt_number, base, offset, logger)
                else:
                    raise

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
) -> Generator[Account, None, None]:
    response = client.get_following(
        account_id=follower.identifier,
        count=config['max_followed_scraped'],
        page_size=config['follows_page_size'],
    )

    accounts = response['accounts'] if 'accounts' in response else []
    return (account_from_obj(account) for account in accounts)


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
