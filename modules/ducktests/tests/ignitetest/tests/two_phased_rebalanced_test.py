# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains Cellular Affinity tests.
"""
import math
import os
from typing import List
from ducktape.cluster.cluster import ClusterNode

from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration, Affinity

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST_2_10, LATEST_2_9, LATEST_2_8

NUM_NODES_CELL = 4

NUM_CELL = 1

ATTRIBUTE = "CELL"

CACHE_NAME = "test-cache"


# pylint: disable=W0223
class TwoPhasedRebalancedTest(IgniteTest):
    """
    Two-phase rebalancing test case.
    """
    # pylint: disable=R0914
    @cluster(num_nodes=(NUM_NODES_CELL * NUM_CELL) + 1)
    @ignite_versions(str(DEV_BRANCH), str(LATEST_2_10), str(LATEST_2_9), str(LATEST_2_8))
    def two_phased_rebalancing_test(self, ignite_version):
        """
        Test case of two-phase rebalancing.
        Preparations.
            1. Start cells.
            2. Load data to cache with the mentioned above affinity function.
            3. Delete 80% of data and measure PDS size on all nodes.
        Phase 1.
            1. Stop two nodes in each cell, total a half of all nodes and clean PDS.
            2. Start cleaned node with preservance of consistent id and cell attributes.
            3. Wait for the rebalance to complete.
        Phase 2.
            Run steps 1-3 of Phase 1 on the other half of the cluster.
        Verifications.
            1. Check that PDS size reduced (compare to step 3)
            2. Check data consistency (idle_verify --dump)
        """
        config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                     data_storage=DataStorageConfiguration(
                                         wal_mode='NONE',
                                         default=DataRegionConfiguration(persistent=True)),
                                     caches=[CacheConfiguration(
                                         name=CACHE_NAME, backups=2, affinity=Affinity(),
                                         indexed_types=['java.lang.Long', 'byte[]'])],
                                     metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi')

        cluster_size = len(self.test_context.cluster)

        num_cell = math.floor((cluster_size - 1) / NUM_NODES_CELL)

        # Start cells.
        cells = self.start_cells(config, num_cell)

        control_utility = ControlUtility(cells[0])
        control_utility.activate()

        client_config = IgniteConfiguration(client_mode=True,
                                            version=IgniteVersion(ignite_version),
                                            discovery_spi=from_ignite_cluster(cells[0]))

        # Load data to cache.
        app = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            params={"start": 0,
                    "cacheName": "test-cache",
                    "interval": 500 * 1024,
                    "valueSizeKb": 1}
        )
        app.run()
        app.free()

        # Delete 80% of data and measure PDS size on all nodes.
        app = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DeleteDataApplication",
            shutdown_timeout_sec=5 * 60,
            params={"cacheName": "test-cache",
                    "size": 400 * 1024}
        )
        app.start(clean=False)
        app.stop()

        node = cells[0].nodes[0]

        control_utility.deactivate()  # flush dirty pages on disk
        control_utility.activate()

        dump_1 = create_idle_dump_and_copy_to_log_dir(control_utility, node, cells[0].log_dir)

        pds_before = self.get_pds_size(cells, "After Delete 80%, PDS.")

        # Restarting the cluster in twos nodes per a cell with a cleanup and waiting for rebalancing.
        restart_with_clean_idx_node_on_cell_and_await_rebalance(cells, [0, 1])
        restart_with_clean_idx_node_on_cell_and_await_rebalance(cells, [2, 3])

        control_utility.deactivate()  # flush dirty pages on disk
        control_utility.activate()

        pds_after = self.get_pds_size(cells, "After rebalancing complete, PDS.")

        # Check that PDS size reduced.
        for host in pds_after:
            assert pds_after[host] < pds_before[host], f'Host {host}: size after = {pds_after[host]}, ' \
                                                       f'size before = {pds_before[host]}.'

        control_utility.validate_indexes()
        dump_2 = create_idle_dump_and_copy_to_log_dir(control_utility, node, cells[0].log_dir)

        # Check data consistency.
        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert not diff, f"Validation error, files are different. Difference:\n {diff}"

    def start_cells(self, config: IgniteConfiguration, num_cell: int) -> List[IgniteService]:
        """
        Start cells.
        :param config IgniteConfiguration.
        :param num_cell Number of cell.
        :return List of IgniteServices.
        """

        cells = []

        first = IgniteService(self.test_context, config, NUM_NODES_CELL, [f'-D{ATTRIBUTE}=0'],
                              startup_timeout_sec=180)
        first.start()

        discovery_spi = from_ignite_cluster(first)
        config = config._replace(discovery_spi=discovery_spi)

        cells.append(first)

        for i in range(1, num_cell):
            cell = IgniteService(self.test_context, config, NUM_NODES_CELL, [f'-D{ATTRIBUTE}={i}'],
                                 startup_timeout_sec=180,)
            cell.start()

            cells.append(cell)

        return cells

    def get_pds_size(self, cells: [IgniteService], msg: str) -> dict:
        """
        Pds size in megabytes.
        :param cells List of IgniteService.
        :param msg Information message.
        :return dict with hostname -> pds size in megabytes.
        """

        res = {}
        for cell in cells:
            for node in cell.nodes:
                consistent_id = str(node.account.hostname).replace('.', '_').replace('-', '_')
                cmd = f'du -sm {cell.database_dir}/{consistent_id} | ' + "awk '{print $1}'"
                res[node.account.hostname] = int(node.account.ssh_output(cmd).decode("utf-8").rstrip())

        self.logger.info(msg)

        for item in res.items():
            self.logger.info(f'Host: {item[0]}, PDS {item[1]}mb')

        return res


def restart_with_clean_idx_node_on_cell_and_await_rebalance(cells: [IgniteService], idxs: [int]):
    """
    Restart idxs nodes on cells with cleaning working directory and await rebalance.
    :param cells List of IgniteService.
    :param idxs List the index nodes that need to be restarted with cleanup.
    """
    stop_idx_node_on_cell(cells, idxs)
    clean_work_idx_node_on_cell(cells, idxs)
    start_idx_node_on_cell(cells, idxs)

    for cell in cells:
        cell.await_rebalance()


def stop_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Stop idxs nodes on cells.
    :param cells List of IgniteService.
    :param idxs List of index nodes to stop.
    """
    for cell in cells:
        for i in idxs:
            cell.stop_node(cell.nodes[i])

    for cell in cells:
        for i in idxs:
            cell.wait_node(cell.nodes[i])


def clean_work_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Cleaning the working directory on idxs nodes in cells.
    :param cells List of IgniteService.
    :param idxs List of index nodes to clean.
    """
    for cell in cells:
        for i in idxs:
            node = cell.nodes[i]

            assert not cell.pids(node)

            node.account.ssh(f'rm -rf {cell.work_dir}')


def start_idx_node_on_cell(cells: [IgniteService], idxs: [int]):
    """
    Start idxs nodes on cells.
    :param cells List of IgniteService.
    :param idxs List of index nodes to start.
    """
    for cell in cells:
        for i in idxs:
            node = cell.nodes[i]

            cell.start_node(node)

    for cell in cells:
        cell.await_started()


def create_idle_dump_and_copy_to_log_dir(control_utility: ControlUtility, node: ClusterNode, log_dir: str) -> str:
    """
    Creates a dump file and copies it to the log directory.
    :param control_utility ControlUtility.
    :param node ClusterNode.
    :param log_dir Path to log directory.
    :return: Path to idle-verify dump file.
    """
    control_utility.idle_verify()

    dump = control_utility.idle_verify_dump(node)

    node.account.ssh_output(f'cp {dump} {log_dir}')

    return os.path.join(log_dir, os.path.basename(dump))
