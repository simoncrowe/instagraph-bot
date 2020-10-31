from unittest import mock

import networkx as nx
import pytest

from ig_bot.factories import AccountFactory
from ig_bot.graph import (
    add_edges, 
    add_nodes, 
    IN_DEGREE_CENTRALITY,
    accounts_with_centrality
)


@pytest.fixture
def account_one_mock():
    mock_ig_account = mock.Mock()
    mock_ig_account.identifier = '1'
    mock_ig_account.username = 'one'
    mock_ig_account.full_name = 'Account One'

    return mock_ig_account


@pytest.fixture
def account_one():
    return AccountFactory(identifier='1',
                                 username='one', 
                                 full_name='Account One')


@pytest.fixture
def account_two():
    return AccountFactory(identifier='2',
                                 username='two',
                                 full_name='Account Two')


@pytest.fixture
def account_three():
    return AccountFactory(identifier='3',
                                 username='three',
                                 full_name='Account Three')


def test_add_nodes_single(account_one):
    graph = nx.DiGraph()

    add_nodes(graph, account_one)
    
    assert list(nx.nodes(graph)) == [account_one.identifier]
    
    identifiers = nx.get_node_attributes(graph, 'identifier')
    assert identifiers[account_one.identifier] == account_one.identifier 
    usernames = nx.get_node_attributes(graph, 'username')
    assert usernames[account_one.identifier] == account_one.username
    full_names = nx.get_node_attributes(graph, 'fullName')
    assert full_names[account_one.identifier] == account_one.full_name 
    


def test_add_nodes_multiple(account_one, account_two):
    graph = nx.DiGraph()
    
    add_nodes(graph, account_one, account_two)
    
    assert list(nx.nodes(graph)) == [account_one.identifier,
                                     account_two.identifier]
    
    identifiers = nx.get_node_attributes(graph, 'identifier')
    assert identifiers[account_one.identifier] == account_one.identifier 
    assert identifiers[account_two.identifier] == account_two.identifier 
    usernames = nx.get_node_attributes(graph, 'username')
    assert usernames[account_one.identifier] == account_one.username 
    assert usernames[account_two.identifier] == account_two.username 
    full_names = nx.get_node_attributes(graph, 'fullName')
    assert full_names[account_one.identifier] == account_one.full_name 
    assert full_names[account_two.identifier] == account_two.full_name 


def test_add_edges(account_one, account_two, account_three):
    graph = nx.DiGraph()
    expected_edge_to_two = (account_one.identifier, account_two.identifier)
    expected_edge_to_three = (account_one.identifier, account_three.identifier)

    add_edges(graph, account_one, [account_two, account_three])
    
    assert graph.number_of_edges() == 2
    edges = graph.edges([account_one.identifier])
    assert expected_edge_to_two in edges
    assert expected_edge_to_three in edges


def test_accounts_with_centrality(account_one, account_two, account_three):
    graph = nx.DiGraph()
    add_nodes(graph, account_one, account_two, account_three)
    add_edges(graph, account_one, [account_two, account_three])
    add_edges(graph, account_two, [account_three])
    
    result_one, result_two, result_three = accounts_with_centrality(
        graph, IN_DEGREE_CENTRALITY
    )

    assert result_one.identifier == account_one.identifier
    assert result_one.username == account_one.username
    assert result_one.full_name == account_one.full_name
    assert result_one.centrality == 0    
    
    assert result_two.identifier == account_two.identifier
    assert result_two.username == account_two.username
    assert result_two.full_name == account_two.full_name
    assert result_two.centrality == 0.5

    assert result_three.identifier == account_three.identifier
    assert result_three.username == account_three.username
    assert result_three.full_name == account_three.full_name
    assert result_three.centrality == 1

    nx.write_gml(graph, '/home/sc/git/instagraph-bot/ig_bot/tests/three_accounts.gml')

