from unittest import mock

from igramscraper.exception import InstagramException
import pytest

from ig_bot.factories import AccountFactory, AccountSummaryFactory
from ig_bot.scraping import (
    exponential_sleep,
    followed_accounts,
    MaxRateLimitingRetriesExceeded,
)


@pytest.fixture
def account_one_mock():
    mock_ig_account = mock.Mock()
    mock_ig_account.identifier = '1'
    mock_ig_account.username = 'one'
    mock_ig_account.full_name = 'Account One'
    return mock_ig_account


@pytest.fixture
def account_one_summary(account_one_mock):
    return AccountSummaryFactory(
        identifier=account_one_mock.identifier,
        username=account_one_mock.username,
        full_name=account_one_mock.full_name,
    ) 


@pytest.fixture
def account_two_mock():
    mock_ig_account = mock.Mock()
    mock_ig_account.identifier = '2'
    mock_ig_account.username = 'two'
    mock_ig_account.full_name = 'Account Two'
    return mock_ig_account


@pytest.fixture
def account_two_summary(account_two_mock):
    return AccountSummaryFactory(
        identifier=account_two_mock.identifier,
        username=account_two_mock.username,
        full_name=account_two_mock.full_name,
    )


@mock.patch('ig_bot.scraping.time.sleep')
def test_exponential_sleep_sleeps_for_approtiate_durations(mock_sleep):
    for e in range (1, 11):
        exponential_sleep(exponent=e, base=2, offset=10, logger=mock.Mock())

    sleep_durations = tuple(call.args[0] for call in mock_sleep.call_args_list)
    assert sleep_durations == (12, 14, 18, 26, 42, 74, 138, 266, 522, 1034)


@mock.patch('ig_bot.scraping.time.sleep')
def test_expoential_sleep_logs_sleep_duration(mock_sleep):
    mock_logger = mock.Mock()

    exponential_sleep(exponent=12, base=2, offset=10, logger=mock_logger)

    mock_logger.info.assert_called_with(f'Sleeping for 4106 seconds...')


def test_followed_account_summaries_yields_followers(
    account_one_mock,
    account_one_summary,
    account_two_mock,
    account_two_summary,
):
    follower = AccountFactory(identifier='1', username='bot')
    mock_client = mock.Mock()
    mock_client.get_following.return_value = {"accounts": [account_one_mock, account_two_mock]}
    mock_logger = mock.Mock()
    config = {
        'scraping': {'follows_page_size': 100},
        'rate_limit_retries': 5,
        'exponential_sleep_base': 2.05,
        'exponetial_sleep_offset': 10.3,
    }

    followed_generator = followed_accounts(follower, mock_client, config=config, logger=mock_logger)

    assert (account_one_summary, account_two_summary) == tuple(followed_generator)


@mock.patch('ig_bot.scraping.exponential_sleep')
def test_followed_account_stubs_sleeps_and_retries_on_rate_limiting_failure(mock_exponential_sleep):
    follower = AccountFactory(identifier='1', username='bot')
    mock_client = mock.Mock()
    mock_client.get_following.side_effect = InstagramException("429")
    config = {
        'scraping': {'follows_page_size': 100},
        'rate_limit_retries': 5,
        'exponential_sleep_base': 2.05,
        'exponetial_sleep_offset': 10.3,
    }
    mock_logger = mock.Mock()

    with pytest.raises(MaxRateLimitingRetriesExceeded):
        followed_accounts(follower, mock_client, config=config, logger=mock_logger)

    assert mock_exponential_sleep.call_count == 5
    assert mock_logger.exception.call_count == 5

