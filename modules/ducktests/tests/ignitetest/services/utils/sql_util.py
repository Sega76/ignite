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
This module contains JDBC driver wrapper.
"""

import os
import jaydebeapi
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_path import IgnitePath
from ignitetest.utils.version import IgniteVersion


def jdbc_connection(ignite_service: IgniteService, ver: IgniteVersion = None):
    """
    :param ignite_service: IgniteService.
    :param ver: IgniteVersion jdbc driver for connection.
    :return Connection.
    """
    if ver is None:
        ver = ignite_service.config.version

    bin_dir = os.environ.get('SOFT_DIR', '/opt')
    home = os.path.join(bin_dir, IgnitePath(ver).home_dir)
    vstr = ver.vstring

    if ver.is_dev:
        core_jar_path = str("%s/modules/core/target/ignite-core-%s-SNAPSHOT.jar" % (home, vstr))
    else:
        core_jar_path = str("%s/libs/ignite-core-%s.jar" % (home, vstr))

    node = ignite_service.nodes[0]

    url = "jdbc:ignite:thin://" + node.account.externally_routable_ip+"/?distributedJoins=true"

    return jaydebeapi.connect(jclassname='org.apache.ignite.IgniteJdbcThinDriver',
                              url=url,
                              jars=core_jar_path)
