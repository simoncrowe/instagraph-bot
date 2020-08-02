from dataclasses import dataclass


def account_summary_from_obj(obj):
    return AccountSummary(
        identifier=obj.identifier,
        username=obj.username,
        full_name=obj.full_name,
    )


@dataclass(frozen=True)
class AccountSummary:
    identifier: str
    username: str
    full_name: str


@dataclass(frozen=True)
class Account:
    identifier: str
    username: str
    full_name: str
    profile_pic_url: str
    profile_pic_url_hd: str
    biography: str
    external_url: str
    follows_count: int
    followed_by_count: int
    media_count: int
    is_private: bool
    is_verified: bool
    country_block: bool
    has_channel: bool
    highlight_reel_count: bool
    is_business_account: bool
    is_joined_recently: bool
    business_category_name: str
    business_email: str
    business_phone_number: str
    business_address_json: str
    connected_fb_page: str

