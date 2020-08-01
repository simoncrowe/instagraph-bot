from dataclasses import dataclass


def account_stub_from_obj(obj):
    return AccountStub(identifier=obj.identifier, username=obj.username)


@dataclass(frozen=True)
class AccountStub:
    identifier: str
    username: str
    full_name: str
    profile_pic_url: str


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

