"""Marshmallow schmas used for serilising igramscraper models."""

from marshmallow import fields, Schema


class Account(Schema):
    """Schema containing relevant fields from igscraper's Account model."""

    identifier = fields.Integer()


class Comment(Schema):
    """Schema containing relevant fields from igscraper's Comment model."""

    # igramscraper doesn't get comments' like counts, so comments may need to be
    # scraped separately-- perhaps using Selenium.
    # TODO: solve this ^

    identifier = fields.Integer()
    text = fields.String()
    created_at = fields.String()
    owner = fields.Nested(Account)


class Image(Schema):
    """Schema containing relevant fields from igscraper's Media model."""

    identifier = fields.Integer()
    short_code = fields.String()
    url = fields.Str(load_from='image_high_resolution_url')
    caption = fields.String()
    likes_count = fields.Integer()
    comments_count = fields.Integer()
    comments = fields.Nested(Comment, many=True)

