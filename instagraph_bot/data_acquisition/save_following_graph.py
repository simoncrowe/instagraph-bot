from datetime import datetime
import logging
from os import path
from typing import List, Dict

import click
from igramscraper.instagram import Instagram
import networkx as nx
import yaml

from model import AccountNode
from ig_util import get_authenticated_igramscraper, random_sleep

from data_acquisition.util import get_graph_file_path


def get_followed_accounts(
        client: Instagram,
        all_account_nodes: Dict[str, AccountNode],
        follower: AccountNode,
        config: dict,
        logger: logging.Logger,
) -> List[AccountNode]:
    """Gets account nodes for accounts followed by a given account."""
    try:
        logger.info(f'Getting accounts followed by "{follower.username}"...')
        following = client.get_following(
            account_id=follower.identifier,
            count=follower.follows_count,
            page_size=config['scraping']['follows_page_size'],
        )['accounts']
    except Exception:
        logger.exception(
            f'Failed to get accounts followed by "{follower.username}".'
        )
        return []

    # Get full details of followed accounts
    followed_nodes = []
    for account in following:
        if account.identifier in all_account_nodes:
            followed_nodes.append(all_account_nodes[account.identifier])
            logger.info(f'Retrieved existing data for "{account.username}".')
        else:
            logger.info(f'Getting user data for "{account.username}"...')
            try:
                account_node = AccountNode.from_igramscraper_account(
                    client.get_account(account.username)
                )
                all_account_nodes[account_node.identifier] = account_node
                followed_nodes.append(account_node)
            except Exception:
                logger.exception(
                    f'Failed to get user data for "{account.username}".'
                )

            logger.info(f'Node for "{account.username}" created.')
            random_sleep(**config['sleep']['after_getting_followed_account'])

    return followed_nodes


def add_following_to_graph(
        graph: nx.DiGraph,
        follower: AccountNode,
        followed_nodes: List[AccountNode],
        logger: logging.Logger,
):
    """Adds edges to graph for followed accounts, returns added AccountNodes."""
    for node in followed_nodes:
        if node.identifier not in graph:
            graph.add_node(node.identifier, **node.to_gml_safe_dict())
            logger.info(f'Created node "{node}".')
        else:
            logger.info(f'Node "{node}" already in graph.')

    for followed in followed_nodes:
        logger.info(f'Created edge from {follower} to {followed}.')
        graph.add_edge(
            u_of_edge=follower.identifier,
            v_of_edge=followed.identifier
        )


def save_graph_gml(graph: nx.Graph, path: str, logger: logging.Logger):
    logger.info('Serialising graph...')
    # Couldn't resist the 'clever' lambda stringize nonsense below.
    nx.write_gml(graph, path, lambda v: ('', v)[bool(v)])
    logger.info(f'Graph saved to {path}')


@click.command()
@click.option('--username', '-u', help='Username of root node.', required=True)
@click.option('--depth', '-d', type=int, help='Depth of search.', required=True)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def save_following_graph(username: str, depth: int, log_level):
    """Scrapes Instagram for a graph of users followed by a user
     and those they follow etc.
     """
    if depth < 1:
        raise ValueError('The depth argument should be 1 or greater.')

    logging.basicConfig(level=log_level)
    logger = logging.getLogger(
        'instagraph_bot.data_acquisition.save_following_graph'
    )

    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    logger.info('Authenticating to Instagram...')
    ig_client = get_authenticated_igramscraper(**config['instagram_auth'])
    random_sleep(**config['sleep']['after_logging_in'])

    logger.info(f'Getting target account: {username}.')
    root_account = AccountNode.from_igramscraper_account(
        ig_client.get_account(username)
    )
    all_account_nodes = {root_account.identifier: root_account}
    random_sleep(**config['sleep']['after_getting_target_account'])

    graph = nx.DiGraph()
    layer = 0
    # Ensure that the root node is in the graph.
    graph.add_node(root_account.identifier, **root_account.to_gml_safe_dict())
    target_accounts = [root_account]

    graph_file_path = get_graph_file_path(
        username,
        type='follows',
        depth=depth,
        data_dir=config['data_directory']
    )
    save_graph_gml(graph=graph, path=graph_file_path, logger=logger)

    while layer < depth:

        next_layer_targets = set()

        for account in target_accounts:
            followed_accounts = get_followed_accounts(
                client=ig_client,
                all_account_nodes=all_account_nodes,
                follower=account,
                config=config,
                logger=logger
            )
            add_following_to_graph(
                graph,
                account,
                followed_accounts,
                logger=logger
            )
            next_layer_targets.update(followed_accounts)
            save_graph_gml(graph=graph, path=graph_file_path, logger=logger)
            random_sleep(2, 4)

        target_accounts = next_layer_targets
        logger.info(f'Layer {layer} complete.')
        layer += 1

    logger.info('Scraping complete.')


if __name__== '__main__':
    save_following_graph()
