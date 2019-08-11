from igramscraper.instagram import Instagram
from time import sleep
from random import random


def get_authenticated_igramscraper(username: str, password: str):
    """Gets an authenticated igramscraper Instagram client instance."""
    client = Instagram()
    client.with_credentials(username, password)
    client.login()
    return client


def random_sleep(minimum: float, maximum: float):
    sleep(minimum + (random() * maximum))
