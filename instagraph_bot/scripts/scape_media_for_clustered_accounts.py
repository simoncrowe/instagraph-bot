import json
from os import path
from typing import Generator, Tuple
import random

from bs4 import BeautifulSoup
import click
import pandas as pd
import requests
import yaml

from scraping import (
    get_authenticated_igramscraper,
    random_sleep
)

from scripts.util import initialise_logger


COMMON_USER_AGENTS = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0 ',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
)


def media_json_from_html(html: BeautifulSoup):
    for script in json_from_scripts(html.find('script'):
        for line in scripts.content:
            # The script in question is a single JS assignment statement
            if 'window._sharedData = ' in line:
            yield json.loads(line[20:-1])


def images(data: dict, user_id: str) -> Generator[Tuple[str, str], None, None]:
    image_id = data["id"]
    sidecar_images = data.get("edge_sidecar_to_children", {}).get("edges")

    if sidecar_images:
        for child_data in sidecar_images:
            child_image_id = child_data["node"]["id"]
            url = child_data["node"]["display_url"]
            yield f"{user_id}/images/{image_id}/children/", url
    else:
        image_id = data["id"]
        url = data["display_url"]
        yield "{user_id}/images/{image_id}/url", url


def comment_data(edges) -> Generator[dict]:
    for edge in edges:
        yield {
             "id": edge["node"]["id"],
             "text": edge["node"]["text"],
             "owner_id": edge["node"]["owner"]["id"],
        }


def comments(data: dict, user_id: str) -> Tuple[str, str]:
    image_id = data["id"]
    edges = data.get("edge_media_to_parent_comment", {}).get("edges")

    comments_json = json.dumps(comment_data(edges))
    return f"{user_id}/images/{image_id}/comments.json", comments_json


def caption(data: dict, user_id: str) -> Tuple:
    edges =  data.get("edge_media_to_caption", {}).get("edges", [{}])
    caption = edges[0].get("node", {}).get("text")
    if caption:
        return f"{user_id}/images/{image_id}/capton.json", json.dumps(caption)


def scrape_shortlink_media(url, user_id, data_path):
    user_agent = random.choice(COMMON_USER_AGENTS)
    reponse = requests.get(url, headers={"User-Agent": user_agent})
    soup = BeautifulSoup(response.text)

    all_data = media_json_from_html(soup)
    media_data = all_data['entry_data']['PostPage'][0]['graphql']['shortcode_media']

    for image_path, image_url in images(media_data, user_id)
        full_image_path = path.join(data_path, image_path)
        save_web_image(full_image_path, image_url)

    comments_path, comments_json = comments(media_data, user_id)
    full_comments_path = path.join(data_path, comments_path)
    save_test(full_comments_path, comments_json)

    caption_path, caption_json = caption(media_data, user_id)
    full_caption_path = path.join(data_path, caption_path)
    save_text(full_caption_path, caption_json)


@click.command()
@click.option('--log-level', '-l', type=str, default='DEBUG')
@click.option(
    '--csv-file',
    '-f',
    'csv_file_path',
    type=str,
    required=True,
    help=(
        'The path to a CSV file containing clustered Instagram account data, '
        'complete with centrality metrics.'
    )
)
@click.option(
    '--cluster-indices',
    '-i',
    type=str,
    default=None,
    help=(
        'Comma-separated list of indices of the clusters for which you want '
        'media scraped. '
        '(If this option is omitted, media for all accounts will be used.)'
    )
)
@click.option(
    '--min-centrality',
    '-c',
    type=float,
    default=0,
    help='The minimum centrality of accounts for which you want media scraped.'
)
def save_media(
        log_level: str,
        csv_file_path: str,
        cluster_indices: str,
        min_centrality: float,
):
    """Scrapes Instagram for media and comments for some or all clustered
    accounts in a CSV file.
     """
    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    data_directory_path = path.join(
        config['data_directory'], 'users',
    )

    logger = initialise_logger(
        directory=config['logs_directory'],
        name=data_directory_name,
        module='instagraph_bot.scripts.scrape_media_for_clustered_accounts',
        level=log_level,
    )

    logger.info(f'Loading data from {data_directory_path}...')
    accounts = pd.read_csv(csv_file_path)

    if cluster_indices is not None:
        cluster_indices = [int(i.strip()) for i in cluster_indices.split(',')]
        accounts = accounts[accounts['cluster'].isin(cluster_indices)]

    accounts = accounts[accounts['centrality'] >= min_centrality]

    logger.info('Authenticating to Instagram...')
    ig_client = get_authenticated_igramscraper(**config['instagram_auth'])
    random_sleep(logger=logger, **config['sleep_ranges']['after_logging_in'])

    for account in accounts.itertuples():
        logger.info(f'Getting media for {account.username}...')

        media = ig_client.get_medias_by_user_id(
            id=account.identifier,
            count=config['scraping']['max_media_items_per_account']
        )
        for media_object in media:
            scrape_shortlink_media(media_object.link, account.id, data_directory_path)


if __name__ == '__main__':
    save_media()
