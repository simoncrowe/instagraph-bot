from unittest import mock

from igramscraper.exception import InstagramException
import pytest

from ig_bot.factories import AccountFactory, AccountSummaryFactory
from ig_bot.scraping import (
    account_by_id,
    account_by_username,
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
    mock_ig_account.profile_pic_url = 'https://1.cdninstagram.com/one.jpg'
    mock_ig_account.profile_pic_url_hd ='https://1.cdninstagram.com/one_hd.jpg'
    mock_ig_account.biography = 'The first account!'
    mock_ig_account.external_url = 'http://account.com/'
    mock_ig_account.follows_count = 1
    mock_ig_account.followed_by_count = 15150045051210
    mock_ig_account.media_count = 16
    mock_ig_account.is_private = True
    mock_ig_account.is_verified = True
    mock_ig_account.country_block = False
    mock_ig_account.has_channel = False
    mock_ig_account.highlight_reel_count = 1
    mock_ig_account.is_business_account = False
    mock_ig_account.is_joined_recently = False
    mock_ig_account.business_category_name = None
    mock_ig_account.business_email = None
    mock_ig_account.business_phone_number = None
    mock_ig_account.business_address_json = None
    mock_ig_account.centrality = None

    return mock_ig_account


@pytest.fixture
def account_one(account_one_mock):
    return AccountFactory(
        identifier=account_one_mock.identifier,
        username=account_one_mock.username,
        full_name=account_one_mock.full_name,
        profile_pic_url=account_one_mock.profile_pic_url,
        profile_pic_url_hd=account_one_mock.profile_pic_url_hd,
        biography=account_one_mock.biography,
        external_url=account_one_mock.external_url,
        follows_count=account_one_mock.follows_count,
        followed_by_count=account_one_mock.followed_by_count,
        media_count=account_one_mock.media_count,
        is_private=account_one_mock.is_private,
        is_verified=account_one_mock.is_verified,
        country_block=account_one_mock.country_block,
        has_channel=account_one_mock.has_channel,
        highlight_reel_count=account_one_mock.highlight_reel_count,
        is_business_account=account_one_mock.is_business_account,
        is_joined_recently=account_one_mock.is_joined_recently,
        business_category_name=account_one_mock.business_category_name,
        business_email=account_one_mock.business_email,
        business_phone_number=account_one_mock.business_phone_number,
        business_address_json=account_one_mock.business_address_json,
        connected_fb_page=account_one_mock.connected_fb_page,
        centrality=account_one_mock.centrality,
    )

@pytest.fixture
def account_one_summary(account_one_mock):
    return AccountSummaryFactory(
        identifier=account_one_mock.identifier,
        username=account_one_mock.username,
        full_name=account_one_mock.full_name,
        centrality=account_one_mock.centrality,
    )


@pytest.fixture
def account_two_mock():
    mock_ig_account = mock.Mock()
    mock_ig_account.identifier = '2'
    mock_ig_account.username = 'two'
    mock_ig_account.full_name = 'Account Two'
    mock_ig_account.centrality = None
    return mock_ig_account


@pytest.fixture
def account_two_summary(account_two_mock):
    return AccountSummaryFactory(
        identifier=account_two_mock.identifier,
        username=account_two_mock.username,
        full_name=account_two_mock.full_name,
        centrality=None,
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


def test_followed_accounts_yields_followers(
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

    followed_generator = followed_accounts(follower, 
                                           mock_client,
                                           config=config, 
                                           logger=mock_logger)

    assert (account_one_summary, account_two_summary) == tuple(followed_generator)


@mock.patch('ig_bot.scraping.exponential_sleep')
def test_followed_accounts_retries_on_rate_limiting(mock_exponential_sleep):
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
        followed_accounts(follower,
                          mock_client, 
                          config=config, 
                          logger=mock_logger)

    assert mock_exponential_sleep.call_count == 5
    assert mock_logger.exception.call_count == 5


def test_account_by_id_returns_account(account_one_mock, account_one):
    mock_client = mock.Mock()
    mock_client.get_account_by_id.return_value = account_one_mock 
    mock_logger = mock.Mock()
    config = {
        'rate_limit_retries': 5,
        'exponential_sleep_base': 2.05,
        'exponetial_sleep_offset': 10.3,
    }

    retrieved_account = account_by_id(account_one.identifier, 
                                      mock_client, 
                                      config=config,
                                      logger=mock_logger)

    assert retrieved_account == account_one


@mock.patch('ig_bot.scraping.exponential_sleep')
def test_account_by_id_retries_on_rate_limiting(mock_exponential_sleep,
                                                account_one):
    mock_client = mock.Mock()
    mock_client.get_account_by_id.side_effect = InstagramException("429")
    config = {
        'scraping': {'follows_page_size': 100},
        'rate_limit_retries': 5,
        'exponential_sleep_base': 2.05,
        'exponetial_sleep_offset': 10.3,
    }
    mock_logger = mock.Mock()

    with pytest.raises(MaxRateLimitingRetriesExceeded):
        account_by_id(account_one.identifier, 
                      mock_client, 
                      config=config,
                      logger=mock_logger)
    
    assert mock_exponential_sleep.call_count == 5
    assert mock_logger.exception.call_count == 5


def test_account_by_username_returns_account(account_one_mock, account_one):
    mock_client = mock.Mock()
    mock_client.get_account.return_value = account_one_mock 
    mock_logger = mock.Mock()
    config = {
        'rate_limit_retries': 5,
        'exponential_sleep_base': 2.05,
        'exponetial_sleep_offset': 10.3,
    }

    retrieved_account = account_by_username(account_one.username, 
                                            mock_client, 
                                            config=config,
                                            logger=mock_logger)

    assert retrieved_account == account_one

@mock.patch('ig_bot.scraping.exponential_sleep')
def test_account_by_username_retries_on_rate_limiting(mock_exponential_sleep,
                                                account_one):
    mock_client = mock.Mock()
    mock_client.get_account.side_effect = InstagramException("429")
    config = {
        'scraping': {'follows_page_size': 100},
        'rate_limit_retries': 5,
        'exponential_sleep_base': 2.05,
        'exponetial_sleep_offset': 10.3,
    }
    mock_logger = mock.Mock()

    with pytest.raises(MaxRateLimitingRetriesExceeded):
        account_by_username(account_one.username, 
                      mock_client, 
                      config=config,
                      logger=mock_logger)
    
    assert mock_exponential_sleep.call_count == 5
    assert mock_logger.exception.call_count == 5


