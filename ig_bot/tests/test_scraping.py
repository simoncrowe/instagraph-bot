from unittest.mock import Mock

from pytest import fixture

from ig_bot.factories import AccountFactory, AccountStubFactory
from ig_bot.scraping import followed_account_stubs


@fixture
def account_one_mock():
    mock_ig_account = Mock()
    mock_ig_account.identifier = '1'
    mock_ig_account.username = 'one'
    mock_ig_account.full_name = 'Account One'
    return mock_ig_account


@fixture
def account_one_stub(account_one_mock):
    return AccountStubFactory(
        identifier=account_one_mock.identifier,
        username=account_one_mock.username,
        full_name=account_one_mock.full_name,
    ) 


@fixture
def account_two_mock():
    mock_ig_account = Mock()
    mock_ig_account.identifier = '2'
    mock_ig_account.username = 'two'
    mock_ig_account.full_name = 'Account Two'
    return mock_ig_account


@fixture
def account_two_stub(account_two_mock):
    return AccountStubFactory(
        identifier=account_two_mock.identifier,
        username=account_two_mock.username,
        full_name=account_two_mock.full_name,
    )


def test_followers_yields_follower(
    account_one_mock,
    account_one_stub, 
    account_two_mock, 
    account_two_stub,
):
    follower = AccountFactory(identifier='1', username='bot')
    mock_client = Mock()
    mock_client.get_following.return_value = {"accounts": [account_one_mock, account_two_mock]}
    config = {'scraping': {'follows_page_size': 100}}
    mock_logger = Mock()

    followed_generator = followed_account_stubs(follower, mock_client, config, mock_logger)

    assert (account_one_stub, account_two_stub) == tuple(followed_generator)


