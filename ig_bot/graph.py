from collections import defaultdict
from typing import Generator, Iterable, Tuple

import networkx as nx

from ig_bot.data import Account, account_to_camel_case


IN_DEGREE_CENTRALITY = 'IN_DEGREE_CENTRALITY'
EIGENVECTOR_CENTRALITY = 'EIGENVECTOR_CENTRALITY'

CENTRALITY_METRIC_FUNCTIONS = {
    IN_DEGREE_CENTRALITY: nx.in_degree_centrality,
    EIGENVECTOR_CENTRALITY: nx.eigenvector_centrality,
}


def add_nodes(graph: nx.DiGraph, *accounts: Tuple[Account]):
    """Adds nodes to graph for Account instance if not already present. """
    graph.add_nodes_from(
        (account.identifier, account_to_camel_case(account))
        for account in accounts
    )


def add_edges(graph: nx.DiGraph,
              source: Account,
              destinations: Iterable[Account]):
    graph.add_edges_from(
        (source.identifier, destination.identifier)
        for destination in destinations
    )


def _combine(**attributes) -> defaultdict:
    """Combines node attribute dicts into dicts of dicts.
    e.g.
    >>> _combine(k1={0: 'a', 1: 'b'}, k2={0: 'c', 1: 'd'})
    {0: {'k1': 'a', 'k2': 'c', 1}, 1: {'k1': 'b', 'k2': 'd'}}
    """
    combined = defaultdict(dict)

    for name, pairs in attributes.items():
        for k, v in pairs.items():
            combined[k][name] = v

    return combined


def accounts_with_centrality(
    graph: nx.DiGraph, centrality_algorithm: str
) -> Generator[Account, None, None]:
    centrality_function = CENTRALITY_METRIC_FUNCTIONS[centrality_algorithm]

    data_by_id = _combine(centrality=centrality_function(graph),
                          username=nx.get_node_attributes(graph, 'username'),
                          full_name=nx.get_node_attributes(graph, 'fullName'))
    for identifier, data in data_by_id.items():
        try:
            yield Account(identifier=identifier, **data)
        except TypeError:
            raise TypeError(
                f"Unabled to instantiate Account object "
                f"with id {identifier} using data: {data}."
            )
