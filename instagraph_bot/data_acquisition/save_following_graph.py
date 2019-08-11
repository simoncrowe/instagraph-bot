from datetime import datetime
import logging
from os import path
from typing import List, Set

import click
from igramscraper.instagram import Instagram
import networkx as nx
import yaml

from model import AccountNode
from util import get_authenticated_igramscraper, random_sleep

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(
    'instagraph_bot.data_acquisition.save_following_graph'
)

with open('config.yaml') as file_obj:
    config = yaml.safe_load(file_obj)


def get_followed_accounts(
        client: Instagram,
        follower: AccountNode
) -> List[AccountNode]:
    """Gets account nodes for accounts followed by a given account."""
    followed = client.get_followers(
        account_id=follower.identifier,
        count=follower.follows_count,
        page_size=config['scraping']['follows_page_size']
    )['accounts']
    # Get full details of followed accounts
    followed_full = []
    for account in followed:
        followed_full.append(client.get_account(account.username))
        random_sleep(1.25, 3.5)
    # Derive hashable AccountNodes
    return [
        AccountNode.from_igramscraper_account(account)
        for account in followed_full
    ]


def add_following_to_graph(
        graph: nx.DiGraph,
        follower: AccountNode,
        followed_nodes: List[AccountNode]
):
    """Adds edges to graph for followed accounts, returns added AccountNodes."""
    nodes_string = '\n'.join([str(node) for node in followed_nodes])
    logger.info(
        f'Creating edges for following:\n {nodes_string}'
    )

    for node in followed_nodes:
        if node not in graph:
            graph.add_node(node.identifier, **node.to_gml_safe_dict())

    for followed in followed_nodes:
        graph.add_edge(
            u_of_edge=follower.identifier,
            v_of_edge=followed.identifier
        )


@click.command()
@click.option('--username', '-u', help='Username of root node.', required=True)
@click.option('--depth', '-d', type=int, help='Depth of search.', required=True)
def save_following_graph(username: str, depth: int):
    """Scrapes Instagram for a graph of users followed by a user
     and those they follow etc.
     """
    if depth < 1:
        raise ValueError('The depth argument should be 1 or greater.')

    ig_client = get_authenticated_igramscraper(**config['instagram_auth'])
    random_sleep(2, 4)

    logger.info(f'Getting target account: "{username}"')
    target_account = AccountNode.from_igramscraper_account(
        ig_client.get_account(username)
    )
    random_sleep(1.5, 3)

    graph = nx.DiGraph()
    # Ensure that the root node is in the graph
    graph.add_node(target_account.identifier, **target_account.to_gml_safe_dict())
    target_accounts = [target_account]
    layer = 0

    while layer < depth:

        next_layer_targets = set()

        for account in target_accounts:
            logger.info(f'Getting accounts followed by "{account.username}"')
            followed_accounts = get_followed_accounts(ig_client, account)
            add_following_to_graph(graph, account, followed_accounts)
            next_layer_targets.update(followed_accounts)
            random_sleep(2, 4)

        target_accounts = next_layer_targets
        layer += 1

    logger.info('Scraping complete.')

    logger.info('Scraping complete. Serialising graph...')
    graph_filename = f'{datetime.now().isoformat()}_{username}_{depth}.gml'
    graph_file_path = path.join(config['data_directory'], graph_filename)
    # Couldn't resist the 'clever' lambda stringize nonsense below.
    nx.write_gml(graph, graph_file_path, lambda v: ('', v)[bool(v)])
    logger.info(f'Graph picked and saved to {graph_file_path}')
    logger.info('Serialising nodes')


if __name__== '__main__':
    save_following_graph()
