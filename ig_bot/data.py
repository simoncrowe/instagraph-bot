from dataclasses import dataclass


def account_summary_from_obj(obj):
    return AccountSummary(
        identifier=obj.identifier,
        username=obj.username,
        full_name=obj.full_name,
    )

def account_from_obj(obj):
    return Account(
        identifier=obj.identifier,
        username=obj.username,
        full_name=obj.full_name,
        profile_pic_url=obj.profile_pic_url,
        profile_pic_url_hd=obj.profile_pic_url_hd,
        biography=obj.biography,
        external_url=obj.external_url,
        follows_count=obj.follows_count,
        followed_by_count=obj.followed_by_count,
        media_count=obj.media_count,
        is_private=obj.is_private,
        is_verified=obj.is_verified,
        country_block=obj.country_block,
        has_channel=obj.has_channel,
        highlight_reel_count=obj.highlight_reel_count,
        is_business_account=obj.is_business_account,
        is_joined_recently=obj.is_joined_recently,
        business_category_name=obj.business_category_name,
        business_email=obj.business_email,
        business_phone_number=obj.business_phone_number,
        business_address_json=obj.business_address_json,
        connected_fb_page=obj.connected_fb_page,
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

