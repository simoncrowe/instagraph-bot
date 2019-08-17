from igramscraper.model.account import Account


class AccountNode:
    """Representation of an Instagram account based on igramscraper."""

    @classmethod
    def from_igramscraper_account(cls, account: Account):
        return AccountNode(
            identifier=account.identifier,
            username=account.username,
            full_name=account.full_name,
            profile_pic_url=account.profile_pic_url,
            profile_pic_url_hd=account.profile_pic_url_hd,
            biography=account.biography,
            external_url=account.external_url,
            follows_count=account.follows_count,
            followed_by_count=account.followed_by_count,
            media_count=account.media_count,
            is_private=account.is_private,
            is_verified=account.is_verified,
            country_block=account.country_block,
            has_channel=account.has_channel,
            highlight_reel_count=account.highlight_reel_count,
            is_business_account=account.is_business_account,
            is_joined_recently=account.is_joined_recently,
            business_category_name=account.business_category_name,
            business_email=account.business_email,
            business_phone_number=account.business_phone_number,
            business_address_json=account.business_address_json,
            connected_fb_page=account.connected_fb_page,
        )

    def __init__(
            self,
            identifier: str,
            username: str,
            full_name: str = None,
            profile_pic_url: str = None,
            profile_pic_url_hd: str = None,
            biography: str = None,
            external_url: str = None,
            follows_count: int = None,
            followed_by_count: int = None,
            media_count: int = None,
            is_private: bool = None,
            is_verified: bool = None,
            country_block: bool = None,
            has_channel: bool = None,
            highlight_reel_count: bool = None,
            is_business_account: bool = None,
            is_joined_recently: bool = None,
            business_category_name: str = None,
            business_email: str = None,
            business_phone_number: str = None,
            business_address_json: str = None,
            connected_fb_page: str = None,
    ):
        self._identifier = identifier
        self._username = username
        self._full_name = full_name
        self._profile_pic_url = profile_pic_url
        self._profile_pic_url_hd = profile_pic_url_hd
        self._biography = biography
        self._external_url = external_url
        self._follows_count = follows_count
        self._followed_by_count = followed_by_count
        self._media_count = media_count
        self._is_private = is_private
        self._is_verified = is_verified
        self._country_block = country_block
        self._has_channel = has_channel
        self._highlight_reel_count = highlight_reel_count
        self._is_business_account = is_business_account
        self._is_joined_recently = is_joined_recently
        self._business_category_name = business_category_name
        self._business_email = business_email
        self._business_phone_number = business_phone_number
        self._business_address_json = business_address_json
        self._connected_fb_page = connected_fb_page

    def to_gml_safe_dict(self) -> dict:
        return {
            'identifier': self.identifier,
            'username': self.username,
            'fullName': self.full_name,
            'profilePicUrl': self.profile_pic_url,
            'profilePicUrlHd': self.profile_pic_url_hd,
            'biography': self.biography,
            'externalUrl': self.external_url,
            'followsCount': self.follows_count,
            'followedByCount': self.followed_by_count,
            'mediaCount': self.media_count,
            'isPrivate': self.is_private,
            'isVerified': self.is_verified,
            'countryBlock': self.country_block,
            'hasChannel': self.has_channel,
            'highlightReelCount': self.highlight_reel_count,
            'isBusinessAccount': self.is_business_account,
            'isJoinedRecently': self.is_joined_recently,
            'businessCategoryName': self.business_category_name,
            'businessEmail': self.business_email,
            'businessPhoneNumber': self.business_phone_number,
            'businessAddressJson': self.business_address_json,
            'connectedFbPage': self.connected_fb_page,
        }

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def username(self) -> str:
        return self._username

    @property
    def full_name(self) -> str:
        return self._full_name

    @property
    def profile_pic_url(self) -> str:
        return self._profile_pic_url

    @property
    def profile_pic_url_hd(self) -> str:
        return self._profile_pic_url_hd

    @property
    def biography(self) -> str:
        return self._biography

    @property
    def external_url(self) -> str:
        return self._external_url

    @property
    def follows_count(self) -> int:
        return self._follows_count

    @property
    def followed_by_count(self) -> int:
        return self._followed_by_count

    @property
    def media_count(self) -> int:
        return self._media_count

    @property
    def is_private(self) -> bool:
        return self._is_private

    @property
    def is_verified(self) -> bool:
        return self._is_verified

    @property
    def country_block(self) -> bool:
        return self._country_block

    @property
    def has_channel(self) -> bool:
        return self._has_channel

    @property
    def highlight_reel_count(self) -> int:
        return self._highlight_reel_count

    @property
    def is_business_account(self) -> bool:
        return self._is_business_account

    @property
    def is_joined_recently(self) -> bool:
        return self._is_joined_recently

    @property
    def business_category_name(self) -> str:
        return self._business_category_name

    @property
    def business_email(self) -> str:
        return self._business_email

    @property
    def business_phone_number(self) -> str:
        return self._business_phone_number

    @property
    def business_address_json(self) -> str:
        return self._business_address_json

    @property
    def connected_fb_page(self) -> str:
        return self._connected_fb_page

    def __hash__(self):
        # TODO: Determine whether hashablity is actually useful
        return hash(self.identifier)

    def __str__(self):
        return self.username or self.full_name
