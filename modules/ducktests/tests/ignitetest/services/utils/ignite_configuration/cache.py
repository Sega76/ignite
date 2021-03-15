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
# limitations under the License

"""
This module contains classes and utilities for Ignite Cache configuration.
"""
from typing import NamedTuple

AFFINITY_BACKUP_FILTER = 'BACKUP_FILTER'
CELL = 'CELL'


class Affinity(NamedTuple):
    """
    Affinity.
    """
    name: str = AFFINITY_BACKUP_FILTER
    attr_name: str = CELL


class CacheConfiguration(NamedTuple):
    """
    Ignite Cache configuration.
    """
    name: str
    cache_mode: str = 'PARTITIONED'
    atomicity_mode: str = 'ATOMIC'
    backups: int = 0
    indexed_types: list = None
    affinity: Affinity = None
