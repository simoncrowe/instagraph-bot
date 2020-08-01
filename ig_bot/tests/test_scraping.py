from unittest.mock import Mock, patch

from ig_bot.factories import AccountFactory, AccountStubFactory
from ig_bot.scraping import followed_account_stubs


def test_followers_yields_follower():
    follower = AccountFactory(identifier='1', username='bot')
    expected_followed = AccountStubFactory(identifier='2', username='bat')
    mock_ig_account = Mock()
    mock_ig_account.identifier = expected_followed.identifier
    mock_ig_account.username = expected_followed.username
    mock_client = Mock()
    mock_client.get_following.return_value = {"accounts": [mock_ig_account]}
    config = {'scraping': {'follows_page_size': 100}}
    mock_logger = Mock()

    followed = next(followed_account_stubs(follower, mock_client, config, mock_logger))

    assert followed == expected_followed

