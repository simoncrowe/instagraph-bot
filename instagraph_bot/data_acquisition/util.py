from datetime import datetime
from os import path


def get_graph_file_path(username: str, type: str, depth: int, data_dir: str):
    graph_filename = (
        f'{datetime.now().isoformat()}_{username}_{type}_{depth}.gml'
    )
    return path.join(data_dir, graph_filename)


