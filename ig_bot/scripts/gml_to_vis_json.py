
import json
from os import path
import yaml

import click
import networkx as nx


from scripts.util import initialise_logger, save_graph_gml


@click.command()
@click.option(
    '--graph', '-g', 'graph_path', required=True, help='Path to GML file.')
@click.option('--log-level', '-l', type=str, default='DEBUG')
def gml_to_json(
        graph_path: str,
        log_level: str
):
    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    base_file_name = (
        f'{path.splitext(path.basename(graph_path))[0]}'
    )

    logger = initialise_logger(
        directory=config['logs_directory'],
        name=base_file_name,
        module='instagraph_bot.scripts.prune_graph_by_centrality',
        level=log_level,
    )

    graph = nx.read_gml(graph_path)

    label_id_map = {label: id_ for id_, label in enumerate(graph)}
    nodes = [{'id': label_id_map[label], 'label': label} for label in graph]
    edges = [{'from': label_id_map[from_], 'to': label_id_map[to]}
             for from_, to in graph.edges]

    json_path = path.join(config['data_directory'], f'{base_file_name}.json')
    logger.info(f'Saving to {json_path}')
    with open(json_path, 'w') as file_obj:
        json.dump({'nodes': nodes, 'edges': edges}, file_obj)


if __name__ == '__main__':
    gml_to_json()
