"""Contains social graph related logic."""

import logging
from typing import Generator, Tuple

import networkx as nx

from ig_bot.data import AccountSummary, account_summary_to_camel_case


CENTRALITY_METRIC_FUNCTIONS = {
    'IN_DEGREE_CENTRALITY': nx.in_degree_centrality,
    'EIGENVECTOR_CENTRALITY': nx.eigenvector_centrality,
}


def add_nodes(graph: nx.DiGraph, *nodes: Tuple[AccountSummary], logger: logging.Logger):
    """Adds nodes to graph for AccountSummary instance is not already present. """
    for node in nodes:
        if node.identifier not in graph:
            graph.add_node(node.identifier, 
                           **account_summary_to_camel_case(node))
            logger.info(f'Added  "{node}" to graph.')
        else:
            logger.info(f'Node "{node}" already in graph.')


def add_edges(graph: nx.DiGraph, 
              source: AccountSummary, 
              *destinations: Tuple[AccountSummary], 
              logger: logging.Logger):
    for destination in destinations:
        graph.add_edge(u_of_edge=source.identifier, v_of_edge=destination.identifier)
        logger.info(f'Created edge from {source} to {followed}.')


def account_nodes_from_graph(
        graph: nx.DiGraph, logger: logging.Logger
) -> Generator[AccountSummary, None, None]:
    pass

