#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from github import Auth, BadCredentialsException, Github, NamedUser

from airflow.models import Connection
from airflow.providers.github.hooks.github import GithubHook
from airflow.utils import db

pytestmark = pytest.mark.db_test

github_client_mock = Mock(name="github_client_for_test")


class TestGithubHook:
    def setup_class(self):
        connections = [
            Connection(
                conn_id="github_default",
                conn_type="github",
                password="my-access-token",
                host="https://mygithub.com/api/v3",
            ),
            Connection(
                conn_id="github_token",
                conn_type="github",
                password="my-access-token",
                host="https://mygithub.com/api/v3",
                extra='{"auth_method": "token"}',
            ),
            Connection(
                conn_id="github_app_installation_auth",
                conn_type="github",
                password="",
                host="https://mygithub.com/api/v3",
                extra='{"auth_method": "app_installation_auth", "app_id": 12345, "private_key": '
                '"my-private-key", "installation_id": 67890}',
            ),
            Connection(
                conn_id="github_app_installation_auth_with_permissions",
                conn_type="github",
                password="",
                host="https://mygithub.com/api/v3",
                extra='{"auth_method": "app_installation_auth", "app_id": 12345, "private_key": '
                '"my-private-key", "installation_id": 67890, "token_permissions": {"issues": "write", '
                '"contents": "read"}}',
            ),
        ]
        for connection in connections:
            db.merge_conn(connection)

    @pytest.mark.parametrize(
        "conn_id, auth_type",
        [
            ("github_default", Auth.Token),
            ("github_token", Auth.Token),
            ("github_app_installation_auth", Auth.AppInstallationAuth),
            ("github_app_installation_auth_with_permissions", Auth.AppInstallationAuth),
        ],
    )
    @patch(
        "airflow.providers.github.hooks.github.GithubClient", autospec=True, return_value=github_client_mock
    )
    def test_github_client_connection(self, github_mock, conn_id, auth_type):
        github_hook = GithubHook(conn_id)

        assert github_mock.called
        assert isinstance(github_mock.call_args.kwargs["auth"], auth_type)
        assert isinstance(github_hook.client, Mock)
        assert github_hook.client.name == github_mock.return_value.name

    @pytest.mark.parametrize(
        "conn_id",
        [
            "github_default",
            "github_token",
            "github_app_installation_auth",
            "github_app_installation_auth_with_permissions",
        ],
    )
    def test_connection_success(self, conn_id):
        hook = GithubHook(conn_id)
        hook.client = Mock(spec=Github)
        hook.client.get_user.return_value = NamedUser.NamedUser

        status, msg = hook.test_connection()

        assert status is True
        assert msg == "Successfully connected to GitHub."

    @pytest.mark.parametrize(
        "conn_id",
        [
            "github_default",
            "github_token",
            "github_app_installation_auth",
            "github_app_installation_auth_with_permissions",
        ],
    )
    def test_connection_failure(self, conn_id):
        hook = GithubHook(conn_id)
        hook.client.get_user = Mock(
            side_effect=BadCredentialsException(
                status=401,
                data={"message": "Bad credentials"},
                headers={},
            )
        )
        status, msg = hook.test_connection()

        assert status is False
        assert msg == '401 {"message": "Bad credentials"}'
