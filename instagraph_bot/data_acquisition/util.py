import logging
from os import path

import networkx as nx
import pandas as pd


def initialise_logger(
        directory: str,
        name: str,
        module: str,
        level: str,
) -> logging.Logger:
    logging.basicConfig(level=level)
    logger = logging.getLogger(module)
    file_handler = logging.FileHandler(path.join(directory, f'{name}.log'))
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    return logger


def get_graph_file_path(directory: str, filename: str):
    return path.join(directory, f'{filename}.gml')


def save_graph_gml(graph: nx.Graph, filepath: str, logger: logging.Logger):
    logger.info('Serialising graph...')
    # Couldn't resist the 'clever' lambda stringize nonsense below.
    nx.write_gml(graph, f'{filepath}.gml', lambda v: ('', v)[bool(v)])
    logger.info(f'Graph saved to {filepath}.')


def load_graph_gml(filepath: str, logger: logging.Logger):
    logger.info(f'Loading graph from {filepath}...')
    graph = nx.read_gml(filepath)
    logger.info('Graph file loaded.')
    return graph


def save_dataframe_csv(df: pd.DataFrame, filepath: str, logger: logging.Logger):
    df.to_csv(filepath)
    logger.info(f'DataFrame saved to {filepath}.')
