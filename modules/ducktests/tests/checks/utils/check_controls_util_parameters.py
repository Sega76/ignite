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
Checks Control Utility params pasrsing
"""

import pytest
from ignitetest.services.utils.auth.creds_provider import CredsProvider
from ignitetest.services.utils.ssl.ssl_factory import SslContextFactory, DEFAULT_ADMIN_KEYSTORE
from ignitetest.services.utils.control_utility import ControlUtility


class Cluster:
    """
    Need this to initialise ControlUtility
    """

    def __init__(self, globals_dict):
        self.context = Context(globals_dict)
        self.certificate_dir = '/opt/certs/'


class Context:
    """
    Need this to initialise ControlUtility
    """

    def __init__(self, globals_dict):
        self.logger = ''
        self.globals = globals_dict


def compare_ssl(class1, class2):
    """
    Compare two SslContextFactory objects
    """

    if class1 is None and class2 is None:
        return True
    if isinstance(class1, SslContextFactory) and isinstance(class2, SslContextFactory):
        return class1.__dict__ == class2.__dict__
    return False


def compare_creds(class1, class2):
    """
    Compare two CredsProvider objects
    """

    if class1 is None and class2 is None:
        return True
    if isinstance(class1, CredsProvider) and isinstance(class2, CredsProvider):
        return class1.__dict__ == class2.__dict__
    return False


class TestParams:
    """
    Globals and Kwargs for tests
    """

    dict_test_ssl_jks = {'key_store_jks': 'admin1.jks', 'key_store_password': 'qwe123',
                         'trust_store_jks': 'truststore.jks', 'trust_store_password': 'qwe123'}
    dict_test_ssl_path = {'key_store_path': '/opt/certs/admin1.jks', 'key_store_password': 'qwe123',
                          'trust_store_path': '/opt/certs/truststore.jks', 'trust_store_password': 'qwe123'}
    dict_test_ssl_only_key = {'key_store_jks': 'admin1.jks'}

    dict_test_creds = {'login': 'admin1', 'password': 'qwe123'}
    dict_test_creds_only_login = {'login': 'admin1'}

    expected_ssl = SslContextFactory(key_store_jks='admin1.jks',
                                     key_store_password='qwe123',
                                     trust_store_password='qwe123')
    expected_ssl_path = SslContextFactory(**dict_test_ssl_path)
    expected_ssl_default = SslContextFactory(key_store_jks=DEFAULT_ADMIN_KEYSTORE)
    expected_ssl_only_key = SslContextFactory(key_store_jks='admin1.jks')

    expected_creds = CredsProvider(login='admin1', password='qwe123')
    expected_creds_default = CredsProvider()
    expected_creds_only_login = CredsProvider(login='admin1')


class CheckCaseGlobalsSetKwargsNotSetSsl:
    """
    Check that control_utulity.py correctly parse SSL params from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected',
                             [({'use_ssl': True,
                                'admin': {'ssl': TestParams.dict_test_ssl_jks}}, TestParams.expected_ssl),
                              ({'use_ssl': True,
                                'admin': {'ssl': TestParams.dict_test_ssl_path}}, TestParams.expected_ssl_path),
                              ({'use_ssl': True,
                                'admin': {'ssl': TestParams.dict_test_ssl_only_key}}, TestParams.expected_ssl_only_key),
                              ({'admin': {'ssl': TestParams.dict_test_ssl_jks}}, None), ])
    def check_parse(test_globals, expected):
        """
        Check that control_utulity.py correctly parse SSL params from globals
        """

        assert compare_ssl(ControlUtility(Cluster(test_globals)).ssl_context, expected)


class CheckCaseGlobalsNotSetKwargsSetSsl:
    """
    Check that control_utulity.py correctly parse SSL params from kwargs
    """

    @staticmethod
    @pytest.mark.parametrize('test_kwargs, expected',
                             [(TestParams.dict_test_ssl_jks, TestParams.expected_ssl),
                              (TestParams.dict_test_ssl_path, TestParams.expected_ssl_path),
                              (TestParams.dict_test_ssl_only_key, TestParams.expected_ssl_only_key)])
    def check_parse(test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse SSL params from kwargs
        """

        assert compare_ssl(ControlUtility(Cluster({}), ssl_context=SslContextFactory(**test_kwargs)).ssl_context,
                           expected)


class CheckCaseGlobalsSetKwargsSetSsl:
    """
    Check that control_utulity.py correctly parse SSL params
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_kwargs, expected',
                             [({'use_ssl': True,
                                'admin': {'ssl': TestParams.dict_test_ssl_jks}}, TestParams.dict_test_ssl_only_key,
                               TestParams.expected_ssl),
                              ({'admin': {'ssl': TestParams.dict_test_ssl_jks}}, TestParams.dict_test_ssl_only_key,
                               TestParams.expected_ssl_only_key)
                              ])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse SSL params
        """

        assert compare_ssl(ControlUtility(Cluster(test_globals), ssl_context=SslContextFactory(**test_kwargs))
                           .ssl_context, expected)


class CheckCaseGlobalsSetKwargsNotSetCreds:
    """
    Check that control_utulity.py correctly parse credentials from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected',
                             [({'use_auth': True,
                                'admin': {'creds': TestParams.dict_test_creds}}, TestParams.expected_creds),
                              ({'use_auth': True,
                                'admin': {'creds': TestParams.dict_test_creds_only_login}},
                               TestParams.expected_creds_only_login),
                              ({'use_auth': True},
                               TestParams.expected_creds_default)])
    def check_parse(test_globals, expected):
        """
        Check that control_utulity.py correctly parse credentials from globals
        """

        assert compare_creds(ControlUtility(Cluster(test_globals)).creds, expected)


class CheckCaseGlobalsNotSetKwargsSetCreds:
    """
    Check that control_utulity.py correctly parse credentials from kwargs
    """

    @staticmethod
    @pytest.mark.parametrize('test_kwargs, expected',
                             [(TestParams.dict_test_creds, TestParams.expected_creds),
                              (TestParams.dict_test_creds_only_login, TestParams.expected_creds_only_login)])
    def check_parse(test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse credentials from kwargs
        """

        assert compare_creds(ControlUtility(Cluster({}), creds=CredsProvider(**test_kwargs)).creds, expected)


class CheckCaseGlobalsSetKwargsSetCreds:
    """
    Check that control_utulity.py correctly parse credentials
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_kwargs, expected',
                             [({'use_auth': True,
                                'admin': {'creds': TestParams.dict_test_creds}}, TestParams.dict_test_creds_only_login,
                               TestParams.expected_creds),
                              ({'admin': {'creds': TestParams.dict_test_creds}}, TestParams.dict_test_creds_only_login,
                               TestParams.expected_creds_only_login)])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse credentials
        """

        assert compare_creds(ControlUtility(Cluster(test_globals), creds=CredsProvider(**test_kwargs)).creds, expected)


class CheckCaseCredsSetSslSet:
    """
    Check that control_utulity.py correctly parse SSL and Credentials
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected_creds, expected_ssl',
                             [({'use_auth': True,
                                'use_ssl': True,
                                'admin': {'creds': TestParams.dict_test_creds, 'ssl': TestParams.dict_test_ssl_jks}},
                               TestParams.expected_creds, TestParams.expected_ssl),
                              ({'use_auth': True,
                                'use_ssl': True},
                               TestParams.expected_creds_default, TestParams.expected_ssl_default)])
    def check_parse(test_globals, expected_creds, expected_ssl):
        """
        Check that control_utulity.py correctly parse credentials
        """

        assert compare_creds(ControlUtility(Cluster(test_globals)).creds, expected_creds)
        assert compare_ssl(ControlUtility(Cluster(test_globals)).ssl_context, expected_ssl)
