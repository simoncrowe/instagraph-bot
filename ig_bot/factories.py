import factory

from ig_bot.data import Account, AccountSummary


class AccountFactory(factory.Factory):
    class Meta:
        model = Account
    
    identifier = '1'
    username = 'janed'
    full_name = 'Jane Doe'
    profile_pic_url = 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07'
    profile_pic_url_hd = 'https://scontent-lht6-1.cdninstagram.com/v/t51.2885-19/s150x150/105988514_720111785229143_1716065946988954927_n.jpg?_nc_ht=scontent-lht6-1.cdninstagram.com&_nc_ohc=nvD5PDjaJOEAX91xG80&oh=2f4a2f789e2f66938babde42c5fbc3fe&oe=5F4EEC07'
    biography = 'Twanscendentaw object'
    external_url = 'http://noumenon.com/' 
    follows_count = 1515
    followed_by_count = 1515 
    media_count = 12
    is_private = True
    is_verified = True
    country_block = False
    has_channel = False
    highlight_reel_count = 1
    is_business_account = False
    is_joined_recently = False
    business_category_name = None
    business_email = None
    business_phone_number = None
    business_address_json = None
    connected_fb_page = None
    

class AccountSummaryFactory(factory.Factory):
    class Meta:
        model = AccountSummary

    identifier = '1'
    username = 'janed'
    full_name = 'Jane Doe'

