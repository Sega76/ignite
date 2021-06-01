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
Module contains in-memory rebalance tests.
"""
from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.rebalance import preload_data, start_ignite, get_result, TriggerEvent, NUM_NODES
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST


# pylint: disable=W0223
class RebalanceInMemoryTest(IgniteTest):
    """
    Tests rebalance scenarios in in-memory mode.
    """
    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_join(self, ignite_version,
                       backups, cache_count, entry_count, entry_size, preloaders,
                       thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance on node join.
        """
        return self.__run(ignite_version, TriggerEvent.NODE_JOIN,
                          backups, cache_count, entry_count, entry_size, preloaders,
                          thread_pool_size, batch_size, batches_prefetch_count, throttle)

    # pylint: disable=too-many-arguments, too-many-locals
    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(backups=[1], cache_count=[1], entry_count=[15_000], entry_size=[50_000], preloaders=[1],
              thread_pool_size=[None], batch_size=[None], batches_prefetch_count=[None], throttle=[None])
    def test_node_left(self, ignite_version,
                       backups, cache_count, entry_count, entry_size, preloaders,
                       thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Tests rebalance on node left.
        """
        return self.__run(ignite_version, TriggerEvent.NODE_LEFT,
                          backups, cache_count, entry_count, entry_size, preloaders,
                          thread_pool_size, batch_size, batches_prefetch_count, throttle)

    # pylint: disable=too-many-arguments, too-many-locals
    def __run(self, ignite_version, trigger_event,
              backups, cache_count, entry_count, entry_size, preloaders,
              thread_pool_size, batch_size, batches_prefetch_count, throttle):
        """
        Test performs rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Triggering a rebalance event and awaits for rebalance to finish.
        :param ignite_version: Ignite version.
        :param trigger_event: Trigger event.
        :param backups: Backup count.
        :param cache_count: Cache count.
        :param entry_count: Cache entry count.
        :param entry_size: Cache entry size.
        :param preloaders: Preload application nodes count.
        :param thread_pool_size: rebalanceThreadPoolSize config property.
        :param batch_size: rebalanceBatchSize config property.
        :param batches_prefetch_count: rebalanceBatchesPrefetchCount config property.
        :param throttle: rebalanceThrottle config property.
        :return: Rebalance and data preload stats.
        """
        ignites = start_ignite(self.test_context, ignite_version, trigger_event, backups, cache_count,
                               entry_count, entry_size, preloaders, thread_pool_size, batch_size,
                               batches_prefetch_count, throttle)

        preload_time = preload_data(
            self.test_context,
            ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            preloaders, backups, cache_count, entry_count, entry_size)

        if trigger_event:
            ignites.stop_node(ignites.nodes[-1])
            ignite = ignites
        else:
            ignite = IgniteService(self.test_context,
                                   ignites.config._replace(discovery_spi=from_ignite_cluster(ignites)), num_nodes=1)
            ignite.start()

        return get_result(ignite.nodes, preload_time, cache_count, entry_count, entry_size)
