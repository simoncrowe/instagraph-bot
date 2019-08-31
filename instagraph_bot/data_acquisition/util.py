import logging
from os import path

import networkx as nx


def initialise_logger(directory: str, name: str, level: str) -> logging.Logger:
    logging.basicConfig(level=level)
    logger = logging.getLogger(
        'instagraph_bot.data_acquisition.save_following_graph'
    )
    file_handler = logging.FileHandler(path.join(directory, f'{name}.log'))
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    return logger


def get_graph_file_path(directory: str, filename: str):
    return path.join(directory, filename)


def save_graph_gml(graph: nx.Graph, filepath: str, logger: logging.Logger):
    logger.info('Serialising graph...')
    # Couldn't resist the 'clever' lambda stringize nonsense below.
    nx.write_gml(graph, f'{filepath}.gml', lambda v: ('', v)[bool(v)])
    logger.info(f'Graph saved to {filepath}.gml.')


def load_graph_gml(filepath: str, logger: logging.Logger):
    logger.info(f'Loading graph from {filepath}...')
    graph = nx.read_gml(filepath)
    logger.info('Graph file loaded.')
    return graph
