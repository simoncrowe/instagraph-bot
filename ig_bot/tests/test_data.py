from collections import defaultdict
from itertools import chain

import pandas as pd
import pytest

from ig_bot.data import (
    Account,
    accounts_from_dataframe,
    accounts_to_dataframe,
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
        'centrality': 0.01,
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
def accounts_dataframe(account_one_data, account_two_data):
    data = defaultdict(list)

    for k, v in chain(account_one_data.items(), account_two_data.items()):
        data[k].append(v)
    
    return pd.DataFrame(data, 
                        (account_one_data['identifier'],
                         account_two_data['identifier']))


def test_accounts_from_dataframe(accounts_dataframe, account_one, account_two):
    result_one, result_two = accounts_from_dataframe(accounts_dataframe)
    
    assert result_one == account_one
    assert result_two == account_two


def test_accounts_to_dataframe(accounts_dataframe, account_one, account_two):
    result = accounts_to_dataframe([account_one, account_two])

    assert all(result == accounts_dataframe)

