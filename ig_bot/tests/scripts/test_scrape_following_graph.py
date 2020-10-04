from collections import defaultdict
from datetime import datetime
from itertools import chain
from mock import patch
from os import mkdir, path
import tempfile

from freezegun import freeze_time
import networkx as nx
import pandas as pd
import pytest

from ig_bot.data import Account, AccountSummary, account_summary_from_obj
from ig_bot.graph import IN_DEGREE_CENTRALITY
from ig_bot.scripts.scrape_following_graph import (
    add_accounts_to_data,
    full_accounts_with_centrality,
    novel_account_stubs,
    record_date_scraped,
    scrape_graph,
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
        'profile_pic_url': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'profile_pic_url_hd': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'biography': 'I am number one!',
        'external_url': 'https://one.me',
        'follows_count': 21,
        'followed_by_count': 7,
        'media_count': 300,
        'is_private': False,
        'is_verified': False,
        'country_block': False,
        'has_channel': True,
        'highlight_reel_count': 52,
        'is_business_account': False,
        'is_joined_recently': True,
        'business_category_name': None,
        'business_email': None,
        'business_phone_number': None,
        'business_address_json': None,
        'connected_fb_page': None,
        'centrality': 0.1,
        'date_scraped': None,
    }

@pytest.fixture
def account_one_data_max_centrality(account_one_data):
    account_one_data['centrality'] = 1
    return account_one_data

@pytest.fixture
def account_one(account_one_data):
    return Account(**account_one_data)


@pytest.fixture
def account_one_max_centrality(account_one_data_max_centrality):
    return Account(**account_one_data_max_centrality)


@pytest.fixture
def account_one_summary(account_one):
    return account_summary_from_obj(account_one)


@pytest.fixture
def account_one_summary_max_centrality(account_one_max_centrality):
    return account_summary_from_obj(account_one_max_centrality)


@pytest.fixture
def account_one_data_with_date_scraped(account_one_data):
    account_one_data_scraped = account_one_data.copy()
    account_one_data_scraped['date_scraped'] = datetime(year=2020, month=10, day=4)
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
        'profile_pic_url': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'profile_pic_url_hd': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'biography': 'Two is company ;-)',
        'external_url': 'https://two.me',
        'follows_count': 255,
        'followed_by_count': 74561,
        'media_count': 4851,
        'is_private': False,
        'is_verified': True,
        'country_block': False,
        'has_channel': True,
        'highlight_reel_count': 0,
        'is_business_account': True,
        'is_joined_recently': False,
        'business_category_name': 'General Interest',
        'business_email':' hi@twoiscompany.xxx',
        'business_phone_number':  '+447885780327',
        'business_address_json': None,
        'connected_fb_page': None,
        'centrality': 0.04,
        'date_scraped': None,
    }


@pytest.fixture
def account_two(account_two_data):
    return Account(**account_two_data)


@pytest.fixture
def account_two_summary(account_two):
    return account_summary_from_obj(account_two)


@pytest.fixture
def account_two_data_with_date_scraped(account_two_data):
    account_two_data_scraped = account_two_data.copy()
    account_two_data_scraped['date_scraped'] = datetime(year=2020, month=10, day=5)
    return account_two_data_scraped


@pytest.fixture
def account_two_with_date_scraped(account_two_data_with_date_scraped):
    return Account(**account_two_data_with_date_scraped)


@pytest.fixture
def account_three_data():
    return {
        'identifier': '3',
        'username': 'three',
        'full_name': 'User Three',
        'profile_pic_url': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'profile_pic_url_hd': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'biography': 'Three is a crowd.',
        'external_url': 'https://threeisacrowd.net',
        'follows_count': 4452,
        'followed_by_count': 7411,
        'media_count': 223,
        'is_private': False,
        'is_verified': True,
        'country_block': False,
        'has_channel': True,
        'highlight_reel_count': 0,
        'is_business_account': True,
        'is_joined_recently': False,
        'business_category_name': 'General Interest',
        'business_email': 'contact@threeisacrowd.net',
        'business_phone_number':  None,
        'business_address_json': None,
        'connected_fb_page': None,
        'centrality': 0.005,
        'date_scraped': None
    }


@pytest.fixture
def account_three(account_three_data):
    return Account(**account_three_data)


@pytest.fixture
def account_three_summary(account_three):
    return account_summary_from_obj(account_three)


@pytest.fixture
def account_four_data():
    return {
        'identifier': '4',
        'username': 'four',
        'full_name': 'User Four',
        'profile_pic_url': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'profile_pic_url_hd': 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07',
        'biography': 'Four!',
        'external_url': 'https://four.zone',
        'follows_count': 84,
        'followed_by_count': 741,
        'media_count': 55,
        'is_private': False,
        'is_verified': True,
        'country_block': False,
        'has_channel': True,
        'highlight_reel_count': 0,
        'is_business_account': True,
        'is_joined_recently': False,
        'business_category_name': 'General Interest',
        'business_email': 'info@four.space',
        'business_phone_number':  None,
        'business_address_json': None,
        'connected_fb_page': None,
        'centrality': 0.001,
        'date_scraped': None
    }


@pytest.fixture
def account_four(account_four_data):
    return Account(**account_four_data)


@pytest.fixture
def account_four_summary(account_four):
    return account_summary_from_obj(account_four)


def dataframe_from_account_data(*data):
    indices = (datum['identifier'] for datum in data)

    data_by_field = defaultdict(list)
    for k, v in chain(*(datum.items() for datum in data)):
        data_by_field[k].append(v)

    return pd.DataFrame(data_by_field, indices)


@pytest.fixture
def first_two_accounts_dataframe(account_one_data, account_two_data):
    return dataframe_from_account_data(account_one_data, account_two_data)


@pytest.fixture
def first_two_accounts_dataframe_account_one_max_centrality(
    account_one_data_max_centrality, account_two_data
):
    return dataframe_from_account_data(account_one_data_max_centrality,
                                       account_two_data)


@pytest.fixture
def first_two_accounts_dataframe_one_scraped(
    account_one_data_with_date_scraped, account_two_data
):
    return dataframe_from_account_data(account_one_data_with_date_scraped,
                                       account_two_data)


@pytest.fixture
def first_two_accounts_dataframe_both_scraped(
    account_one_data_with_date_scraped,
    account_two_data_with_date_scraped,
):
    return dataframe_from_account_data(account_one_data_with_date_scraped,
                                       account_two_data_with_date_scraped)


@pytest.fixture
def first_three_accounts_dataframe(
    account_one_data,
    account_two_data,
    account_three_data
):
    return dataframe_from_account_data(account_one_data,
                                       account_two_data,
                                       account_three_data)


@pytest.fixture
def first_three_accounts_dataframe_first_two_scraped(
    account_one_data_with_date_scraped,
    account_two_data_with_date_scraped,
    account_three_data,
):
    return dataframe_from_account_data(account_one_data_with_date_scraped,
                                       account_two_data_with_date_scraped,
                                       account_three_data)


def test_record_scraping_date_sets_date_on_appropriate_row(
    first_two_accounts_dataframe_one_scraped,
    first_two_accounts_dataframe_both_scraped,
    account_two,
    account_two_with_date_scraped,
):
    with freeze_time(account_two_with_date_scraped.date_scraped):
        resulting_data = record_date_scraped(
            first_two_accounts_dataframe_one_scraped,
            account_two
        )

    assert resulting_data.equals(first_two_accounts_dataframe_both_scraped)



def test_novel_account_stubs_includes_missing_account_summary(
    first_two_accounts_dataframe,
    account_three_summary,
    account_three
):
    summaries_filter = novel_account_stubs(first_two_accounts_dataframe,
                                           [account_three_summary],
                                           1)

    assert list(summaries_filter) == [account_three_summary]


def test_novel_account_stubs_filters_out_excess_account_summary(
    first_two_accounts_dataframe,
    account_three_summary,
    account_three,
    account_four_summary,
):
    summaries_filter = novel_account_stubs(first_two_accounts_dataframe,
                                           [account_three_summary, 
                                            account_four_summary],
                                           1)
    summaries = list(summaries_filter)

    assert account_three_summary in summaries
    assert account_four_summary.identifier not in summaries

def test_update_centrality_updates_as_expected(
        first_two_accounts_dataframe,
        first_two_accounts_dataframe_account_one_max_centrality,
        account_one_summary_max_centrality
):
    resulting_data = update_centrality(
        first_two_accounts_dataframe, [account_one_summary_max_centrality]
    )

    assert resulting_data.equals(
        first_two_accounts_dataframe_account_one_max_centrality
    )


def test_add_accounts_to_data_returns_full_dataframe(
    first_two_accounts_dataframe,
    first_three_accounts_dataframe,
    account_three
):
    resulting_dataframe = add_accounts_to_data(first_two_accounts_dataframe,
                                               [account_three])

    assert resulting_dataframe.equals(first_three_accounts_dataframe)


def test_top_scraping_candidate_returns_appropriate_account(
	first_three_accounts_dataframe_first_two_scraped, account_three
):
    resulting_account = top_scraping_candidate(
        first_three_accounts_dataframe_first_two_scraped, 3
    )

    assert resulting_account == account_three


def test_top_scraping_candidate_returns_none_if_max_accounts_already_scraped(
    first_three_accounts_dataframe_first_two_scraped
):
    resulting_account = top_scraping_candidate(
        first_three_accounts_dataframe_first_two_scraped, 2
    )

    assert resulting_account == None


def test_top_scraping_candidate_returns_none_if_all_accounts_scraped(
	first_two_accounts_dataframe_both_scraped
):
    resulting_account = top_scraping_candidate(
        first_two_accounts_dataframe_both_scraped, 3
    )

    assert resulting_account == None


@patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
def test_scrape_graph_no_username_and_nonexistent_data_dir(_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'this_dir_does_not_exist')

        with pytest.raises(ValueError):
            scrape_graph(data_path, 1000)


@patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
def test_scrape_graph_no_username_and_empty_data_dir(_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)

        with pytest.raises(ValueError):
            scrape_graph(data_path, 1000)


@patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
@patch('ig_bot.scripts.scrape_following_graph.load_dataframe_csv')
@patch('ig_bot.scripts.scrape_following_graph.load_graph_gml')
def test_scrape_graph_username_and_files_in_data_dir(*_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)

        graph_path = path.join(data_path, 'graph.gml')
        open(graph_path, 'w').close()

        accounts_path = path.join(data_path, 'accounts.csv')
        open(accounts_path, 'w').close()

        with pytest.raises(ValueError):
            scrape_graph(data_path, 1000, 'some_user')


@freeze_time("2020-10-04 18:08:25")
@patch('ig_bot.scripts.scrape_following_graph.get_authenticated_igramscraper')
@patch('ig_bot.scripts.scrape_following_graph.followed_accounts')
@patch('ig_bot.scripts.scrape_following_graph.account_by_id')
@patch('ig_bot.scripts.scrape_following_graph.account_by_username')
@patch('ig_bot.scripts.scrape_following_graph._load_config')
def test_scrape_graph_writes_graph_and_data_to_dir_with_username(
        mock_load_config,
        mock_account_by_username,
        mock_account_by_id,
        mock_followed_accounts,
        _mock_get_authenticated_igramscraper,
        account_one,
        account_one_summary,
        account_two,
        account_two_summary,
        account_three,
        account_three_summary,
        account_four,
        account_four_summary,
):
    config = {
        'ig_auth': {
            'username': 'foo',
            'password': 'bar',
        },
        'rate_limit_retries': 3,
        'scraping': {
            'follows_page_size': 5
        }
    }
    mock_load_config.return_value = config

    accounts_by_username = {account_one.username: account_one}
    mock_account_by_username.side_effect = accounts_by_username.get

    accounts_by_id = {
        account_two.identifier: account_two,
        account_three.identifier: account_three,
        account_four.identifier: account_four,
    }
    mock_account_by_id.side_effect = accounts_by_id.get

    followed_accounts_map =  {
        account_one.identifier: [account_two_summary],
        account_two.identifier: [account_one_summary, account_three_summary],
        account_three.identifier: [
            account_one_summary,
            account_four_summary
        ],
    }

    def fake_followed_accounts(account, ig_client, config, logger):
        return followed_accounts_map.get(account.identifier)

    mock_followed_accounts.side_effect = fake_followed_accounts

    expected_graph_path = path.join(TEST_DATA_DIR, 'all-four-accounts.gml')
    expected_graph = nx.read_gml(expected_graph_path)

    expected_csv_path = path.join(TEST_DATA_DIR, 'top-three-accounts.csv')
    expected_accounts_data = pd.read_csv(expected_csv_path)

    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        graph_path = path.join(data_path, 'graph.gml')
        accounts_path = path.join(data_path, 'accounts.csv')

        scrape_graph(data_path, 3, 'one', IN_DEGREE_CENTRALITY)

        accounts_data = pd.read_csv(accounts_path)
        accounts_data.to_csv('~/git/instagraph-bot/ig_bot/tests/scripts/data/top-three-accounts-actual.csv', index=False)
        assert accounts_data.equals(expected_accounts_data)

        graph = nx.read_gml(graph_path)
        assert graph.nodes == expected_graph.nodes
        assert graph.edges == expected_graph.edges

