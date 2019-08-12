from datetime import datetime
import logging
from os import path
from typing import List, Dict

import click
from igramscraper.instagram import Instagram
from igramscraper.model.account import Account
import networkx as nx
import yaml

from model import AccountNode
from ig_util import get_authenticated_igramscraper, random_sleep

from data_acquisition.util import get_graph_file_path

IMPORTANCE_MEASURES = {
    'IN_DEGREE_CENTRALITY': nx.in_degree_centrality,
    'EIGENVECTOR_CENTRALITY': nx.eigenvector_centrality,
}


def get_followed_accounts(
        client: Instagram,
        follower: AccountNode,
        config: dict,
        logger: logging.Logger,
) -> List[Account]:
    """Gets minimal information for accoutns followed by a given account."""
    try:
        logger.info(f'Getting accounts followed by "{follower.username}"...')
        return client.get_following(
            account_id=follower.identifier,
            count=follower.follows_count,
            page_size=config['scraping']['follows_page_size'],
        )['accounts']
    except Exception:
        logger.exception(
            f'Failed to get accounts followed by "{follower.username}".'
        )
        return []


def get_nodes_for_accounts(
        client: Instagram,
        accounts: List[Account],
        all_account_nodes: Dict[str, AccountNode],
        config: dict,
        logger: logging.Logger,
) -> List[AccountNode]:
    """Get nodes for accounts with full information."""
    nodes = []
    for account in accounts:
        if account.identifier in all_account_nodes:
            nodes.append(all_account_nodes[account.identifier])
            logger.info(f'Retrieved existing data for "{account.username}".')
        else:
            logger.info(f'Getting user data for "{account.username}"...')
            try:
                account_node = AccountNode.from_igramscraper_account(
                    client.get_account(account.username)
                )
                all_account_nodes[account_node.identifier] = account_node
                nodes.append(account_node)
            except Exception:
                logger.exception(
                    f'Failed to get user data for "{account.username}".'
                )

            logger.info(f'Node for "{account.username}" created.')
            random_sleep(**config['sleep']['after_getting_followed_account'])

    return nodes


def add_nodes(
        graph: nx.DiGraph,
        nodes: List[AccountNode],
        logger: logging.Logger,
):
    """Adds edges to graph for followed accounts, returns added AccountNodes."""
    for node in nodes:
        if node.identifier not in graph:
            graph.add_node(node.identifier, **node.to_gml_safe_dict())
            logger.info(f'Created node "{node}".')
        else:
            logger.info(f'Node "{node}" already in graph.')

def add_edges(
        graph: nx.DiGraph,
        source: AccountNode,
        destinations: List[AccountNode],
        logger: logging.Logger,
):
    for followed in destinations:
        logger.info(f'Created edge from {source} to {followed}.')
        graph.add_edge(
            u_of_edge=source.identifier,
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
@click.option(
    '--prune-unimportant-accounts-after',
    '-p',
    type=int,
    default=2,
    help='The layer after which to prune unimportant accounts.'
)
@click.option(
    '--max-important-accounts-kept',
    '-m',
    type=int,
    default=128,
    help='How many accounts to keep in layers where pruning takes place.'
)
@click.option(
    '--importance-measure',
    '-i',
    type=str,
    default='EIGENVECTOR_CENTRALITY',
    help='The measure determining the importance of an account.'
)
@click.option(
    '--exclude-first-layer',
    is_flag=True,
    help='Exclude the root node (account) and edges to accounts it follows.'
)
def save_following_graph(
        username: str,
        depth: int,
        log_level,
        prune_unimportant_accounts_after,
        max_important_accounts_kept,
        importance_measure,
        exclude_first_layer,

):
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
    if not exclude_first_layer:
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
            accounts_following = get_followed_accounts(
                client=ig_client,
                follower=account,
                config=config,
                logger=logger
            )
            nodes_following = get_nodes_for_accounts(
                client=ig_client,
                accounts=accounts_following,
                all_account_nodes=all_account_nodes,
                config=config,
                logger=logger
            )

            add_nodes(
                graph=graph,
                nodes=nodes_following,
                logger=logger
            )
            if not (exclude_first_layer and layer == 0):
                add_edges(
                    graph=graph,
                    source=account,
                    destinations=nodes_following,
                    logger=logger
                )
            next_layer_targets.update(accounts_following)

            save_graph_gml(graph=graph, path=graph_file_path, logger=logger)

        if layer >= prune_unimportant_accounts_after:

            # TODO: Refactor into function.
            account_importance = IMPORTANCE_MEASURES[importance_measure](graph)
            important_account_identifiers = [
                account_id for account_id, _ in sorted(
                    account_importance,
                    key=lambda _, importance: importance,
                    reverse=True
                )
                if all_account_nodes[account_id] not in target_accounts
            ][:max_important_accounts_kept]
            target_accounts = [
                all_account_nodes[identifier]
                for identifier in important_account_identifiers
            ]

        else:
            target_accounts = next_layer_targets
        logger.info(f'Layer {layer} complete.')
        layer += 1
        random_sleep(**config['sleep']['after_adding_layer'])

    logger.info('Scraping complete.')


if __name__== '__main__':
    save_following_graph()
