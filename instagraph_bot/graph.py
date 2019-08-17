"""Contains social graph related logic."""

import logging
from typing import Callable, Collection, Dict, List

import networkx as nx

from model import AccountNode


def order_account_nodes_by_importance(
        graph: nx.DiGraph,
        all_account_nodes: Dict[str, AccountNode],
        candidate_account_nodes: Collection[AccountNode],
        importance_measure: Callable,
        logger:logging.Logger,
) -> List[AccountNode]:
    account_importance = importance_measure(graph)

    if not any(account_importance.values()):
        logger.warning('All nodes have zero importance. Order is meaningless.')

    important_account_identifiers = [
        account_id for account_id, _ in sorted(
            account_importance.items(),
            key=lambda id_importance_tuple: id_importance_tuple[1],
            reverse=True
        )
        if all_account_nodes[account_id] in candidate_account_nodes
    ]
    return [
        all_account_nodes[identifier]
        for identifier in important_account_identifiers
    ]

