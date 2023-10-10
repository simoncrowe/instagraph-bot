import csv
import json
import logging
import struct
from os import path
from pathlib import Path
from typing import List

import click
import networkx as nx

from ig_bot.data import Account
from ig_bot.scripts.util import initialise_logger, load_graph_graphml


def _load_graph(graph_path: str, logger: logging.Logger):
    try:
        return load_graph_graphml(graph_path, logger)
    except OSError:
        return None


def _load_accounts(accounts_path: str, logger: logging.Logger) -> List[Account]:
    try:
        with open(accounts_path, 'r') as file_obj:
            reader = csv.DictReader(file_obj)
            return [Account(**row_data) for row_data in reader]

    except OSError:
        return None


def _get_logger(data_dir, log_level: str) -> logging.Logger:
    return initialise_logger(data_dir,
                             'log',
                             'ig_bot.scripts.scrape_following_graph',
                             log_level)


@click.command()
@click.argument('data_dir')
@click.argument('output_dir')
@click.option('--node-count', '-n', type=int, default=1000)
@click.option('--graph-path', type=str, required=True)
@click.option('--log-level', '-l', type=str, default='INFO')
def generate_visualisation_input(data_dir: str,
                                 output_dir: str,
                                 node_count: int,
                                 graph_path: str,
                                 log_level: str):
    accounts_path = path.join(data_dir, 'accounts.csv')

    logger = _get_logger(data_dir, log_level)
    
    graph = _load_graph(graph_path, logger)
    accounts = _load_accounts(accounts_path, logger)

    if not (graph and accounts):
        logger.error("Data not present in directory.")
        exit(1)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    nodes_path = path.join(output_dir, "nodes.json")
    edges_path = path.join(output_dir, "edges.bin")

    indices_by_account = list(enumerate(accounts, start=0))[:node_count]
    indices_by_identifier = {account.identifier: index
                             for index, account in indices_by_account}    
    
    identifier_by_idx = nx.get_node_attributes(graph, "identifier")
    x_locs = {identifier_by_idx[k]: v 
              for k, v in nx.get_node_attributes(graph, "x").items()}
    y_locs = {identifier_by_idx[k]: v
              for k, v in nx.get_node_attributes(graph, "y").items()}

    nodes_data = []
    with open(edges_path, "wb") as fileobj:
        
        for index, account in indices_by_account:
            logger.info(f"Processing node {index}: {account.username}")
            node_data = {"rank": index + 1,
                         "username": account.username,
                         "name": account.full_name,
                         "x": x_locs[account.identifier],
                         "y": y_locs[account.identifier]} 
            nodes_data.append(node_data)
            edges_from = graph.edges(account.identifier)
            for origin_id, target_id in edges_from:
                if target_index := indices_by_identifier.get(target_id):
                    fileobj.write(struct.pack("<H", target_index))
            
            if index != node_count:
                # Use max unsigned 16 bit int as delimiter
                fileobj.write(struct.pack("<H", 65535))
    
    with open(nodes_path, "w") as fileobj:
        json.dump(nodes_data, fileobj)


if __name__ == "__main__":
    generate_visualisation_input()
