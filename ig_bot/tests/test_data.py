from collections import defaultdict
from datetime import datetime
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
        'centrality': 0.01,
        'date_scraped': datetime(year=2020, month=9, day=6, hour=0, minute=14),
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
        'centrality': 0.04,
        'date_scraped': None,
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

