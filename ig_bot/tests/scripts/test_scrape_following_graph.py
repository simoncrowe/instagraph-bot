from collections import defaultdict
from itertools import chain
from mock import patch
from os import mkdir, path
import tempfile
import pandas as pd
import pytest

from ig_bot.data import Account, AccountSummary, account_summary_from_obj
from ig_bot.scripts.scrape_following_graph import (
    scrape_graph,
    update_accounts_data,
)


@pytest.fixture
def account_one_data():
    return {
        'identifier': 1,
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
    }


@pytest.fixture
def account_one(account_one_data):
    return Account(**account_one_data)


@pytest.fixture
def account_two_data():
    return {
        'identifier': 2,
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
    }


@pytest.fixture
def account_two(account_two_data):
    return Account(**account_two_data)



@pytest.fixture
def account_three_data():
    return {
        'identifier': 3,
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
        'identifier': 4,
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
    }


@pytest.fixture
def account_four(account_four_data):
    return Account(**account_four_data)


@pytest.fixture
def account_four_summary(account_four):
    return account_summary_from_obj(account_four)


@pytest.fixture
def first_two_accounts_dataframe(account_one_data, account_two_data):
    data = defaultdict(list)

    for k, v in chain(account_one_data.items(), account_two_data.items()):
        data[k].append(v)

    return pd.DataFrame(data,
                        (account_one_data['identifier'],
                         account_two_data['identifier']))


@pytest.fixture
def first_three_accounts_dataframe(
    account_one_data,
    account_two_data,
    account_three_data
):
    data = defaultdict(list)

    for k, v in chain(account_one_data.items(),
                      account_two_data.items(),
                      account_three_data.items()):
        data[k].append(v)

    return pd.DataFrame(data,
                        (account_one_data['identifier'],
                         account_two_data['identifier'],
                         account_three_data['identifier']))


@patch('ig_bot.scripts.scrape_following_graph.account_by_id')
def test_update_accounts_data_calls_account_by_id_for_missing_account(
    mock_account_by_id,
    first_two_accounts_dataframe,
    account_three_summary,
    account_three
):
    account_id_map = {account_three.identifier: account_three}
    mock_account_by_id.side_effect = account_id_map.get

    update_accounts_data(first_two_accounts_dataframe,
                         [account_three_summary],
                         1)

    mock_account_by_id.assert_called_with(account_three_summary.identifier)


@patch('ig_bot.scripts.scrape_following_graph.account_by_id')
def test_update_accounts_data_does_not_call_account_by_id_for_excess_account(
    mock_account_by_id,
    first_two_accounts_dataframe,
    account_three_summary,
    account_three,
    account_four_summary,
):
    account_id_map = {account_three.identifier: account_three}
    mock_account_by_id.side_effect = account_id_map.get

    update_accounts_data(first_two_accounts_dataframe,
                         [account_three_summary, account_four_summary],
                         1)

    call_args = [call.args for call in mock_account_by_id.call_args_list]
    assert len(call_args) == 1
    assert (account_three_summary.identifier,) in call_args
    assert (account_four_summary.identifier,) not in call_args



@patch('ig_bot.scripts.scrape_following_graph.account_by_id')
def test_update_accounts_data_returns_full_dataframe(
    mock_account_by_id,
    first_two_accounts_dataframe,
    first_three_accounts_dataframe,
    account_three_summary,
    account_three
):
    account_id_map = {account_three.identifier: account_three}
    mock_account_by_id.side_effect = account_id_map.get

    resulting_dataframe = update_accounts_data(first_two_accounts_dataframe,
                                               [account_three_summary],
                                               1)

    assert all(resulting_dataframe == first_three_accounts_dataframe)


@patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
def test_scrape_graph_no_username_and_nonexistent_data_dir(_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'this_dir_does_not_exist')

        with pytest.raises(ValueError):
            scrape_graph(data_path)


@patch('ig_bot.scripts.scrape_following_graph._load_config', return_value={})
def test_scrape_graph_no_username_and_empty_data_dir(_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)

        with pytest.raises(ValueError):
            scrape_graph(data_path)


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
            scrape_graph(data_path, 'some_user')

