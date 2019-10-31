import os

import click
import yaml
import pandas as pd

from scraping import (
    get_authenticated_igramscraper,
    random_sleep
)

from scripts.util import initialise_logger


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
def save_following_graph(
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

    data_directory_name = f'{os.path.basename(csv_file_path)}_media'
    data_directory_path = os.path.join(
        config['data_directory'],
        data_directory_name,
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
            pass
            # TODO: add Selenium-based logic for grabbing all images & comments


if __name__ == '__main__':
    save_following_graph()
