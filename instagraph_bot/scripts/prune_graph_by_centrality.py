
import yaml
from os import path

import click
import networkx as nx

from graph import CENTRALITY_METRIC_FUNCTIONS

from scripts.util import initialise_logger, save_graph_gml


@click.command()
@click.option(
    '--graph', '-g', 'graph_path', required=True, help='Path to GML file.')
@click.option(
    '--importance-measure',
    '-i',
    type=str,
    default='EIGENVECTOR_CENTRALITY',
    help='The measure determining the importance of an account.'
)
@click.option(
    '--accounts-retained',
    '-r',
    type=int,
    required=True,
    help=(
            'Number of important accounts retained. '
            '(Negative value means all are retained.)'
    )
)
@click.option(
    '--max-followers',
    '-f',
    type=int,
    default=-1,
    help=(
            'The maximum number of followers of retained accounts. '
            'This is intended as a crude means of removing outliers. '
            '(A negative value means that there is no maximum.)'
    )
)
@click.option(
    '--omit-attributes',
    is_flag=True,
    help='Include this tag if you do not want to retain node attributes.'
)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def prune_graph(
        graph_path: str,
        importance_measure: str,
        accounts_retained: int,
        max_followers: int,
        omit_attributes: bool,
        log_level: str
):
    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    base_file_name = (
        f'{path.splitext(path.basename(graph_path))[0]}_'
        f'pruned-to-{accounts_retained}'
    )
    if max_followers:
        base_file_name += f'_max-followers_{max_followers}'
    if omit_attributes:
        base_file_name += '_no-attrs'

    logger = initialise_logger(
        directory=config['logs_directory'],
        name=base_file_name,
        module='instagraph_bot.scripts.prune_graph_by_centrality',
        level=log_level,
    )

    graph = nx.read_gml(graph_path)

    # Negative value for max_followers mean that here is not maximum
    if max_followers > -1:
        for identifier, followers in nx.get_node_attributes(
                graph,
                'followedByCount'
        ).items():
            if followers >= max_followers:
                graph.remove_node(identifier)

    importance = CENTRALITY_METRIC_FUNCTIONS[importance_measure](graph)
    important_identifiers = set([
        identifier for identifier, centrality in
        sorted(importance.items(), key=lambda kv: kv[1], reverse=True)
    ][:accounts_retained])

    for identifier in set(graph).difference(important_identifiers):
        graph.remove_node(identifier)

    label_map = {}
    for identifier, data in list(graph.nodes(data=True)):
        username, name = data['username'], data['fullName']
        label_map[identifier] = f'{name} ({username})' if name else username
    nx.relabel_nodes(graph, label_map, copy=False)

    if omit_attributes:
        for identifier, data in list(graph.nodes(data=True)):
            for key in list(data.keys()):
                del graph.node[identifier][key]

    data_dir = config['data_directory']
    save_graph_gml(
        graph=graph,
        filepath=path.join(data_dir, f'{base_file_name}.gml'),
        logger=logger,
    )


if __name__ == '__main__':
    prune_graph()
