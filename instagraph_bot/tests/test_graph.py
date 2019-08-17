from typing import Dict
from unittest import TestCase, mock

import networkx as nx

from model import AccountNode
from graph import order_account_nodes_by_importance


class GraphTestCase(TestCase):

    def setUp(self):
        """Initialise directional graph with five nodes and no edges."""
        self.bam = AccountNode(identifier='0', username='bam')
        self.bem = AccountNode(identifier='1', username='bem')
        self.bim = AccountNode(identifier='2', username='bim')
        self.bom = AccountNode(identifier='3', username='bom')
        self.bum = AccountNode(identifier='4', username='bum')

        self.all_nodes_dict = {
            self.bam.identifier: self.bam,
            self.bem.identifier: self.bem,
            self.bim.identifier: self.bim,
            self.bom.identifier: self.bom,
            self.bum.identifier: self.bum
        }

        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(
            [self.bam, self.bem, self.bim, self.bom, self.bum]
        )

    def test_order_account_nodes_by_importance_all_zero_centrality(self):
        mock_importance_function = mock.Mock(
            return_value={
                self.bam.identifier: 0,
                self.bim.identifier: 0,
                self.bem.identifier: 0,
                self.bom.identifier: 0,
                self.bum.identifier: 0,
            }
        )
        mock_logger = mock.Mock()
        order_account_nodes_by_importance(
            graph=self.graph,
            all_account_nodes=self.all_nodes_dict,
            candidate_account_nodes=[self.bim, self.bum, self.bom],
            importance_measure=mock_importance_function,
            logger=mock_logger
        ),
        self.assertTrue(mock_logger.warning.called)

    def test_order_account_nodes_by_importance_some_nodes_candidate(self):
        mock_importance_function = mock.Mock(
            return_value={
                self.bam.identifier: 0,
                self.bim.identifier: 0.25,
                self.bem.identifier: 0.5,
                self.bom.identifier: 0.75,
                self.bum.identifier: 1,
            }
        )
        mock_logger = mock.Mock()
        self.assertListEqual(
            order_account_nodes_by_importance(
                graph=self.graph,
                all_account_nodes=self.all_nodes_dict,
                candidate_account_nodes=[self.bim, self.bum, self.bom],
                importance_measure=mock_importance_function,
                logger=mock_logger
            ),
            [self.bum, self.bom, self.bim]
        )

    def test_order_account_nodes_by_importance_all_nodes_candidate(self):
        mock_importance_function = mock.Mock(
            return_value={
                self.bam.identifier: 0,
                self.bim.identifier: 0.25,
                self.bem.identifier: 0.5,
                self.bom.identifier: 0.75,
                self.bum.identifier: 1,
            }
        )
        mock_logger = mock.Mock()
        self.assertListEqual(
            order_account_nodes_by_importance(
                graph=self.graph,
                all_account_nodes=self.all_nodes_dict,
                candidate_account_nodes=[
                    self.bim,
                    self.bum,
                    self.bam,
                    self.bem,
                    self.bom
                ],
                importance_measure=mock_importance_function,
                logger=mock_logger
            ),
            [self.bum, self.bom, self.bem, self.bim, self.bam]
        )
