import functools
import logging
from random import random
import time
from typing import Generator


from ig_bot.data import Account, account_from_obj
import instagrapi


class MaxRateLimitingRetriesExceeded(Exception):
    """The service still rate limits after the maxiumum number of attempts."""


class NotFound(Exception):
    """Client cannot find an account."""



def get_authenticated_client(username: str, password: str):
    """Gets an authenticated client instance."""
    client = instagrapi.Client()
    client.login(username, password)
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
            except instagrapi.exceptions.ClientError as exception:
                if "404 Client Error" in str(exception):
                    raise NotFound("404 when attempting to get resource. It was probably deleted.")

                logger.error(f"Scraping failed: {exception}")
                base = config['exponential_sleep_base']
                offset = config['exponential_sleep_offset']
                exponential_sleep(attempt_number, base, offset, logger)

        raise MaxRateLimitingRetriesExceeded(
            f"Function {func.__name__} still failed due to rate limiting "
            f"after {attempt_number} attempts."
        )

    return wrapper


@retry_on_rate_limiting
def followed_accounts(
        follower: Account,
        client: instagrapi.Client,
        config: dict,
        logger: logging.Logger
) -> Generator[Account, None, None]:
    results = client.user_following_v1(
        follower.identifier,
        amount=config['max_followed_scraped'],
    )
    return (account_from_obj(account) for account in results)


@retry_on_rate_limiting
def account_by_id(identifier: str,
                  client: instagrapi.Client,
                  config: dict,
                  logger: logging.Logger) -> Account:

    return account_from_obj(client.user_info(identifier))


@retry_on_rate_limiting
def account_by_username(username: str,
                        client: instagrapi.Client,
                        config: dict,
                        logger: logging.Logger) -> Account:

    return account_from_obj(client.user_info_by_username(username))
