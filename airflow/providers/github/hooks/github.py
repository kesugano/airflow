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
"""This module allows you to connect to GitHub."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from github import Auth, Github as GithubClient

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection


class GithubHook(BaseHook):
    """
    Interact with GitHub.

    Performs a connection to GitHub and retrieves client.

    :param github_conn_id: Reference to :ref:`GitHub connection id <howto/connection:github>`.
    """

    conn_name_attr = "github_conn_id"
    default_conn_name = "github_default"
    conn_type = "github"
    hook_name = "GitHub"

    def __init__(self, github_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.github_conn_id = github_conn_id
        self.client: GithubClient | None = None
        self.get_conn()

    def get_conn(self) -> GithubClient:
        """Initiate a new GitHub connection with token and hostname (for GitHub Enterprise)."""
        if self.client is not None:
            return self.client

        conn = self.get_connection(self.github_conn_id)
        auth = self._get_auth(conn)
        host = conn.host

        if not host:
            self.client = GithubClient(auth=auth)
        else:
            self.client = GithubClient(auth=auth, base_url=host)

        return self.client

    @classmethod
    def get_ui_field_behaviour(cls) -> dict:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {
                "host": "GitHub Enterprise URL (Optional)",
                "password": "GitHub Access Token (for Token Auth)",
            },
            "placeholders": {
                "host": "https://{hostname}/api/v3 (for GitHub Enterprise)",
                "extra": json.dumps(
                    {
                        "auth_method": "app_installation_auth",
                        "app_id": 12345,
                        "private_key": "Your App's Private Key",
                        "installation_id": 67890,
                    },
                    indent=2,
                ),
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test GitHub connection."""
        try:
            if TYPE_CHECKING:
                assert self.client
            self.client.get_user().id
            return True, "Successfully connected to GitHub."
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _get_auth(conn: Connection) -> Auth.Auth:
        """Get the appropriate authentication method based on the connection details."""
        auth_method = conn.extra_dejson.get("auth_method", "token")

        if auth_method == "token":
            return GithubHook._get_token_auth(conn)
        elif auth_method == "app_installation_auth":
            return GithubHook._get_app_installation_auth(conn)
        else:
            raise AirflowException(f"Unsupported auth_method: {auth_method}")

    @staticmethod
    def _get_token_auth(conn: Connection) -> Auth.Token:
        """Get the token authentication method."""
        access_token = conn.password
        if not access_token:
            raise AirflowException("An access token is required to authenticate to GitHub.")
        return Auth.Token(access_token)

    @staticmethod
    def _get_app_installation_auth(conn: Connection) -> Auth.AppInstallationAuth:
        """Get the GitHub App installation authentication method."""
        extras = conn.extra_dejson

        if not isinstance(extras.get("app_id"), (int, str)):
            raise AirflowException("app_id must be an integer or string.")
        if not isinstance(extras.get("private_key"), str):
            raise AirflowException("private_key must be a string.")
        if not isinstance(extras.get("installation_id"), int):
            raise AirflowException("installation_id must be an integer.")
        if not isinstance(extras.get("token_permissions"), (dict, type(None))):
            raise AirflowException("token_permissions must be a JSON object or None.")

        app_auth = Auth.AppAuth(extras["app_id"], extras["private_key"])
        return app_auth.get_installation_auth(
            extras["installation_id"], token_permissions=extras.get("token_permissions")
        )
