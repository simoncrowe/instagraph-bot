# -*- coding: UTF-8 -*-
import csv
import json
import logging
import random
from dataclasses import asdict, fields
from datetime import datetime
from itertools import chain, islice
from operator import attrgetter
from os import path
from pathlib import Path
from typing import Iterable, Iterator, List, Union

import click
import yaml

from ig_bot.scraping import (
    account_by_username,
    get_authenticated_client,
    get_authenticated_client_from_session,
    followed_accounts,
    NotFound,
    random_sleep,
)
from ig_bot.scripts.util import initialise_logger, load_graph_gml, save_graph_gml

import instagrapi.exceptions 

def _get_logger(data_dir, log_level: str) -> logging.Logger:
    return initialise_logger(data_dir,
                             'log',
                             'ig_bot.scripts.follow_accounts',
                             log_level)


def _load_config(config_path):
    with open(config_path, 'r') as fileobj:
        return yaml.safe_load(fileobj)


@click.command()
@click.argument("data_dir")
@click.option('--minimum', type=int, help='Minimum number of accounts to follow.')
@click.option('--maximum', type=int, help='Maximum number of accounts to follow.')
@click.option('--config-path', '-c', type=str, default='./config.yaml')
@click.option('--log-level', '-l', type=str, default='INFO')
def follow_accounts(
    data_dir,
    minimum,
    maximum,
    config_path,
    log_level
):
    nodes_path = path.join(data_dir, "nodes.json")
    session_path = path.join(data_dir, "session.json")
    config = _load_config(config_path)
    logger = _get_logger(data_dir, log_level)
    

    credentials = config['ig_auth']
    logger.info(f'Authenticating as {credentials["username"]}')
    ig_client = get_authenticated_client_from_session(**credentials, session_path=session_path, logger=logger)

    sleep_between_accounts = config['sleep']['between_accounts']
    for _ in range(random.randint(minimum, maximum)):
        account_data = None
        with open(nodes_path, "r") as file_obj:
            nodes = json.load(file_obj)
            while account_data is None:
                node = nodes.pop(random.randint(0, len(nodes)-1))
                try:
                    user_id = ig_client.user_id_from_username(node["username"])
                except instagrapi.exceptions.UserNotFound:
                    continue
                node["user_id"] = user_id
                account_data = node
        
        with open(nodes_path, "w") as file_obj:
            json.dump(nodes, file_obj)
        
        ig_client.user_follow(account_data["user_id"])
        logger.info(
            f"Followed {account_data['username']}..."
        )
        random_sleep(**sleep_between_accounts, logger=logger)

    logger.info("Finished! Saving IG session")
    ig_client.dump_settings(session_path)

if __name__ == "__main__":
    follow_accounts()
