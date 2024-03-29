from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import chain
from os import mkdir, path
from unittest import mock
import shutil
import tempfile

from click.testing import CliRunner
from igramscraper.instagram import InstagramNotFoundException
import networkx as nx
import pandas as pd
import pytest
import time_machine

from ig_bot.data import Account, account_from_obj
from ig_bot.scripts.scrape_following_graph import (
    record_date_scraped,
    relevant_new_accounts,
    scrape_following_graph,
    scrape_following_graph_command,
    top_scraping_candidate,
    update_centrality
)

TEST_DATA_DIR = path.join(path.dirname(__file__), 'data')

@pytest.fixture
def account_one_data():
    return {
        'identifier': '1',
        'username': 'one',
        'full_name': 'User One',
        'centrality': 0.1,
        'date_scraped': None,
    }

@pytest.fixture
def account_one(account_one_data):
    return Account(**account_one_data)


@pytest.fixture
def account_one_max_centrality(account_one_data_max_centrality):
    return Account(**account_one_data_max_centrality)


@pytest.fixture
def account_one_data_with_date_scraped(account_one_data):
    account_one_data_scraped = account_one_data.copy()
    account_one_data_scraped['date_scraped'] = datetime(year=2020,
                                                        month=10,
                                                        day=4,
                                                        hour=2,
                                                        minute=44)
    return account_one_data_scraped


@pytest.fixture
def account_one_with_date_scraped(account_one_data_with_date_scraped):
    return Account(**account_one_data_with_date_scraped)


@pytest.fixture
def account_two_data():
    return {
        'identifier': '2',
        'username': 'two',
        'full_name': 'User Two',
        'centrality': 0.04,
        'date_scraped': None,
    }



@pytest.fixture
def account_two(account_two_data):
    return Account(**account_two_data)


@pytest.fixture
def account_two_data_with_date_scraped(account_two_data):
    account_two_data_scraped = account_two_data.copy()
    account_two_data_scraped['date_scraped'] = datetime(year=2020,
                                                        month=10,
                                                        day=5,
                                                        hour=8,
                                                        minute=12)
    return account_two_data_scraped


@pytest.fixture
def account_two_with_date_scraped(account_two_data_with_date_scraped):
    return Account(**account_two_data_with_date_scraped)


@pytest.fixture
def account_two_data_max_centrality(account_two_data):
    account_two_data['centrality'] = 1
    return account_two_data


@pytest.fixture
def account_two_max_centrality(account_two_data_max_centrality):
    return Account(**account_two_data_max_centrality)


@pytest.fixture
def account_three_data():
    return {
        'identifier': '3',
        'username': 'three',
        'full_name': 'User Three',
        'centrality': 0.005,
        'date_scraped': None
    }


@pytest.fixture
def account_three(account_three_data):
    return Account(**account_three_data)


@pytest.fixture
def account_four_data():
    return {
        'identifier': '4',
        'username': 'four',
        'full_name': 'User Four',
        'centrality': 0.001,
        'date_scraped': None
    }


@pytest.fixture
def account_four(account_four_data):
    return Account(**account_four_data)


@pytest.fixture
def account_five_data():
    return {
        'identifier': '5',
        'username': 'five',
        'full_name': 'User Five',
        'centrality': 0,
        'date_scraped': None
    }


@pytest.fixture
def account_five(account_five_data):
    return Account(**account_five_data)


@pytest.fixture
def account_six_data():
    return {
        'identifier': '6',
        'username': 'six',
        'full_name': 'User Six',
        'centrality': 0,
        'date_scraped': None
    }


@pytest.fixture
def account_six(account_six_data):
    return Account(**account_six_data)


def test_record_scraping_date_sets_date_on_appropriate_row(
    account_one_with_date_scraped,
    account_two,
    account_two_with_date_scraped,
):
    accounts = [account_one_with_date_scraped, account_two]
    expected_accounts = [account_one_with_date_scraped,
                         account_two_with_date_scraped]

    with time_machine.travel(account_two_with_date_scraped.date_scraped,
                             tick=False):
        resulting_accounts = list(record_date_scraped(accounts, account_two))

    assert resulting_accounts == expected_accounts


def test_relevant_new_accounts_includes_missing_account(
    account_one, account_two, account_three
):
    resulting_accounts = relevant_new_accounts([account_one, account_two],
                                               [account_three],
                                               1)

    assert list(resulting_accounts) == [account_three]


def test_relevant_new_accounts_filters_out_excess_account(
    account_one, account_two, account_three, account_four
):
    accounts = relevant_new_accounts([account_one, account_two],
                                     [account_three, account_four],
                                     1)
    resulting_accounts = list(accounts)

    assert account_three in resulting_accounts
    assert account_four not in resulting_accounts


def test_update_centrality_updates_as_expected(
        account_one, account_two, account_two_max_centrality
):
    old_accounts = [account_one, account_two]
    new_accounts = [account_one, account_two_max_centrality]
    expected_accounts = [account_two_max_centrality, account_one]

    resulting_accounts = update_centrality(old_accounts, new_accounts)

    assert list(resulting_accounts) == expected_accounts


def test_top_scraping_candidate_returns_appropriate_account(
    account_one_with_date_scraped, account_two_with_date_scraped, account_three
):
    resulting_account = top_scraping_candidate(
       [account_one_with_date_scraped,
        account_two_with_date_scraped,
        account_three],
       3
    )

    assert resulting_account == account_three


def test_top_scraping_candidate_returns_none_if_max_accounts_already_scraped(
    account_one_with_date_scraped, account_two_with_date_scraped, account_three
):
    resulting_account = top_scraping_candidate(
       [account_one_with_date_scraped,
        account_two_with_date_scraped,
        account_three],
       2
    )

    assert resulting_account is None


def test_top_scraping_candidate_returns_none_if_all_accounts_scraped(
    account_one_with_date_scraped, account_two_with_date_scraped
):
    resulting_account = top_scraping_candidate(
        [account_one_with_date_scraped, account_two_with_date_scraped], 3
    )

    assert resulting_account is None


@mock.patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
def test_scrape_graph_command_no_username_and_nonexistent_data_dir(
    mock_load_config
):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'this_dir_does_not_exist')

        command_args = [data_path]
        result = CliRunner().invoke(scrape_following_graph_command, command_args)

        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)

    mock_load_config.assert_called_once()


@mock.patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
def test_scrape_graph_command_no_username_and_empty_data_dir(_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)

        command_args = [data_path]
        result = CliRunner().invoke(scrape_following_graph_command, command_args)

        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)


@mock.patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
@mock.patch('ig_bot.scripts.scrape_following_graph.load_graph_gml')
def test_scrape_graph_command_username_and_files_in_data_dir(
    mock_load_graph, mock_load_config
):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)

        graph_path = path.join(data_path, 'graph.gml')
        open(graph_path, 'w').close()

        accounts_path = path.join(data_path, 'accounts.csv')
        open(accounts_path, 'w').close()

        command_args = [data_path, '--username', 'foo']
        result = CliRunner().invoke(scrape_following_graph_command, command_args)

        assert result.exit_code != 0
        assert result.exception

        mock_load_graph.assert_called_once()
        mock_load_config.assert_called_once()


@time_machine.travel("2020-10-04T18:08:25Z", tick=False)
@mock.patch('ig_bot.scripts.scrape_following_graph.random_sleep')
@mock.patch('ig_bot.scripts.scrape_following_graph.get_authenticated_igramscraper')
@mock.patch('ig_bot.graph.CENTRALITY_METRIC_FUNCTIONS')
@mock.patch('ig_bot.scripts.scrape_following_graph.followed_accounts')
@mock.patch('ig_bot.scripts.scrape_following_graph.account_by_username')
@mock.patch('ig_bot.scripts.scrape_following_graph._load_config')
def test_scrape_graph_writes_graph_and_data_to_dir_with_username(
        mock_load_config,
        mock_account_by_username,
        mock_followed_accounts,
        mock_centrality_function_map,
        mock_get_authenticated_igramscraper,
        mock_time_sleep,
        account_one,
        account_two,
        account_three,
        account_four,
):
    config = {
        'ig_auth': {
            'username': 'foo',
            'password': 'bar',
        },
        'rate_limit_retries': 3,
        'exponential_sleep_base': 6,
        'exponential_sleep_offset': 300,
        'max_followed_scraped': 9999,
        'follows_page_size': 5,
        'accounts_per_batch': {
            'minimum': 4,
            'maximum': 9,
        },
        'sleep': {
            'between_account_batches': {
                'minimum': 300,
                'maximum': 600,
            },
            'between_accounts': {
                'minimum': 15,
                'maximum': 45,
            }
        }
    }
    mock_load_config.return_value = config

    account_by_username_map =  {account_one.username: account_one}

    def account_by_username_stub(username, *args, **kwargs):
        if username not in account_by_username_map:
            raise InstagramNotFoundException
        return account_by_username_map[username]

    mock_account_by_username.side_effect = account_by_username_stub

    followed_map = {
        account_one.identifier: [account_two],
        account_two.identifier: [account_one, account_three],
        account_three.identifier: [account_one, account_four],
    }

    def followed_accounts_stub(account, *args, **kwargs):
        if account.identifier not in followed_map:
            return list()
        return followed_map[account.identifier]

    mock_followed_accounts.side_effect = followed_accounts_stub

    def centrality_function_get_stub(key):
        return nx.in_degree_centrality

    mock_centrality_function_map.__getitem__.side_effect = centrality_function_get_stub

    expected_graph_path = path.join(TEST_DATA_DIR,
                                    'scrape_following_graph',
                                    'expected_data_dir_with_username',
                                    'all-four-accounts.gml')
    expected_graph = nx.read_gml(expected_graph_path)

    expected_csv_path = path.join(TEST_DATA_DIR,
                                  'scrape_following_graph',
                                  'expected_data_dir_with_username',
                                  'top-three-accounts.csv')
    expected_accounts_data = pd.read_csv(expected_csv_path)

    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        graph_path = path.join(data_path, 'graph.gml')
        accounts_path = path.join(data_path, 'accounts.csv')

        scrape_following_graph(data_dir=data_path,
                               username=account_one.username,
                               poorest_centrality_rank=3,
                               config_path='./config.yaml',
                               log_level='INFO')

        accounts_data = pd.read_csv(accounts_path)
        assert accounts_data.equals(expected_accounts_data)

        graph = nx.read_gml(graph_path)
        assert graph.nodes == expected_graph.nodes
        assert graph.edges == expected_graph.edges

    mock_load_config.assert_called_once()
    mock_account_by_username.assert_called()
    mock_followed_accounts.assert_called()
    mock_centrality_function_map.__getitem__.assert_called()
    mock_get_authenticated_igramscraper.assert_called()
    mock_time_sleep.assert_called()

@time_machine.travel("2020-10-04T18:08:25Z", tick=False)
@mock.patch('ig_bot.scripts.scrape_following_graph.random_sleep')
@mock.patch('ig_bot.scripts.scrape_following_graph.get_authenticated_igramscraper')
@mock.patch('ig_bot.graph.CENTRALITY_METRIC_FUNCTIONS')
@mock.patch('ig_bot.scripts.scrape_following_graph.followed_accounts')
@mock.patch('ig_bot.scripts.scrape_following_graph._load_config')
def test_scrape_graph_updates_existing_data_dir(
        mock_load_config,
        mock_followed_accounts,
        mock_centrality_function_map,
        mock_get_authenticated_igramscraper,
        mock_time_sleep,
        account_one,
        account_two,
        account_three,
        account_four,
        account_five,
        account_six,
):
    config = {
        'ig_auth': {
            'username': 'foo',
            'password': 'bar',
        },
        'rate_limit_retries': 3,
        'exponential_sleep_base': 6,
        'exponential_sleep_offset': 300,
        'max_followed_scraped': 9999,
        'follows_page_size': 5,
        'accounts_per_batch': {
            'minimum': 4,
            'maximum': 9,
        },
        'sleep': {
            'between_account_batches': {
                'minimum': 300,
                'maximum': 600,
            },
            'between_accounts': {
                'minimum': 15,
                'maximum': 45,
            }
        }
    }
    mock_load_config.return_value = config

    followed_map = {
        account_one.identifier: [account_two],
        account_two.identifier: [account_one, account_three],
        account_three.identifier: [account_one, account_four],
        account_four.identifier: [account_five, account_six],
        account_five.identifier: [account_four, account_two,
                                  account_six],
        account_six.identifier: [account_one, account_three,
                                 account_four, account_five]
    }

    def fake_followed_accounts(account, *args, **kwargs):
        return followed_map[account.identifier]
 
    mock_followed_accounts.side_effect = fake_followed_accounts

    def fake_centrality_function_get(key):
        return nx.in_degree_centrality

    mock_centrality_function_map.__getitem__.side_effect = fake_centrality_function_get

    starting_graph_path = path.join(TEST_DATA_DIR,
                                    'scrape_following_graph',
                                    'starting_data_dir',
                                    'all-four-accounts.gml')
    starting_csv_path = path.join(TEST_DATA_DIR,
                                  'scrape_following_graph',
                                  'starting_data_dir',
                                  'top-four-accounts.csv')

    expected_graph_path = path.join(TEST_DATA_DIR,
                                    'scrape_following_graph',
                                    'expected_data_dir',
                                    'all-six-accounts.gml')
    expected_graph = nx.read_gml(expected_graph_path)
    expected_csv_path = path.join(TEST_DATA_DIR,
                                  'scrape_following_graph',
                                  'expected_data_dir',
                                  'top-six-accounts.csv')
    expected_accounts_data = pd.read_csv(expected_csv_path)

    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        graph_path = path.join(data_path, 'graph.gml')
        accounts_path = path.join(data_path, 'accounts.csv')

        mkdir(data_path)
        shutil.copy(starting_graph_path, graph_path)
        shutil.copy(starting_csv_path, accounts_path)

        scrape_following_graph(data_dir=data_path,
                               username=None,
                               poorest_centrality_rank=5,
                               config_path='./config.yaml',
                               log_level='INFO')

        accounts_data = pd.read_csv(accounts_path)
        assert accounts_data.equals(expected_accounts_data)

        graph = nx.read_gml(graph_path)
        assert graph.nodes == expected_graph.nodes
        assert graph.edges == expected_graph.edges

    mock_load_config.assert_called_once()
    mock_followed_accounts.assert_called()
    mock_centrality_function_map.__getitem__.assert_called()
    mock_get_authenticated_igramscraper.assert_called()
    mock_time_sleep.assert_called()
