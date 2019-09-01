from colorsys import hls_to_rgb

import click
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import pandas as pd


@click.command()
@click.option(
    '--data-path',
    '-d',
    required=True,
    help='CSV file containing clustered account data.'
)
def visualise_clustered_accounts(data_path):
    data = pd.read_csv(data_path)

    figure = plt.figure()
    axes = figure.add_subplot(111, projection='3d')

    cluster_nums = data['cluster'].unique()
    cluster_hues = np.arange(0, 1, 1 / len(cluster_nums))

    for cluster_num in cluster_nums:
        cluster_data = data.loc[data['cluster'] == cluster_num]
        red, green, blue = hls_to_rgb(cluster_hues[cluster_num], 0.4, 0.95)
        axes.scatter(
            xs=cluster_data['followedByCount'].to_numpy(),
            ys=cluster_data['followsCount'].to_numpy(),
            zs=cluster_data['centrality'].to_numpy(),
            c=np.array([[red, green, blue, 1]])
        )
    axes.set_xlabel('Followed by')
    axes.set_ylabel('Follows')
    axes.set_zlabel('Centrality')

    plt.show()


if __name__ == '__main__':
    visualise_clustered_accounts()
