import logging

import click
import networkx as nx
import yaml

from graph import order_account_nodes_by_importance, add_nodes, add_edges
from model import AccountNode
from scraping import (
    get_authenticated_igramscraper,
    get_followed_accounts,
    get_nodes_for_accounts,
    random_sleep
)

from data_acquisition.util import (
    get_base_filename,
    get_graph_file_path,
    initialise_logger,
    save_graph_gml
)

IMPORTANCE_MEASURE_FUNCTIONS = {
    'IN_DEGREE_CENTRALITY': nx.in_degree_centrality,
    'EIGENVECTOR_CENTRALITY': nx.eigenvector_centrality,
}


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

    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    base_file_name = get_base_filename(
        username,
        type='follows',
        depth=depth,
    )

    logger = initialise_logger(
        directory=config['logs_directory'],
        name=base_file_name,
        level=log_level,
    )

    logger.info('Authenticating to Instagram...')
    ig_client = get_authenticated_igramscraper(**config['instagram_auth'])
    random_sleep(logger=logger, **config['sleep']['after_logging_in'])

    logger.info(f'Getting target account: {username}.')
    root_account = AccountNode.from_igramscraper_account(
        ig_client.get_account(username)
    )
    all_account_nodes = {root_account.identifier: root_account}
    random_sleep(logger=logger, **config['sleep']['after_getting_target_account'])

    graph = nx.DiGraph()
    layer = 0
    if not exclude_first_layer:
        # Ensure that the root node is in the graph.
        graph.add_node(root_account.identifier, **root_account.to_gml_safe_dict())
    target_accounts = [root_account]

    graph_file_path = get_graph_file_path(
        filename=base_file_name,
        directory=config['data_directory'],
    )
    save_graph_gml(graph=graph, path=graph_file_path, logger=logger)

    accounts_already_targeted = set()

    while layer < depth:

        next_layer_targets = set()
        accounts_already_targeted.update(target_accounts)

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
            next_layer_targets.update(nodes_following)

            save_graph_gml(graph=graph, path=base_file_name, logger=logger)

        if layer >= prune_unimportant_accounts_after:
            target_accounts = order_account_nodes_by_importance(
                graph=graph,
                all_account_nodes=all_account_nodes,
                candidate_account_nodes=next_layer_targets.difference(
                    accounts_already_targeted
                ),
                importance_measure=IMPORTANCE_MEASURE_FUNCTIONS[
                    importance_measure
                ],
                logger=logger
            )[:max_important_accounts_kept]

        else:
            target_accounts = next_layer_targets
        logger.info(f'Layer {layer} complete.')
        layer += 1
        random_sleep(logger=logger, **config['sleep']['after_adding_layer'])

    logger.info('Scraping complete.')


if __name__== '__main__':
    save_following_graph()
