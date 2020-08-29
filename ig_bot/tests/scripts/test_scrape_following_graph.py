from unittest.mock import patch
from os import mkdir, path
import tempfile

import pytest

from ig_bot.scripts.scrape_following_graph import scrape_graph


def test_scrape_graph_no_username_and_empty_data_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)
 
        with pytest.raises(ValueError):
            scrape_graph(data_path)

@patch("ig_bot.scripts.scrape_following_graph.load_dataframe_csv")
@patch("ig_bot.scripts.scrape_following_graph.load_graph_gml")
def test_scrape_graph_username_and_files_in_data_diri(*_):
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = path.join(temp_dir, 'data')
        mkdir(data_path)

        graph_path = path.join(data_path, 'graph.gml')
        open(graph_path, 'w').close()
        
        accounts_path = path.join(data_path, 'accounts.csv')
        open(accounts_path, 'w').close()

        with pytest.raises(ValueError):
            scrape_graph(data_path, 'some_user')

