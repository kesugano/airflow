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

import os
import sys
from glob import glob
from pathlib import Path

import pytest

from airflow.models import DagBag
from airflow.utils import yaml
from tests.test_utils.asserts import assert_queries_count

AIRFLOW_SOURCES_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"

NO_DB_QUERY_EXCEPTION = ["/airflow/example_dags/example_subdag_operator.py"]

if os.environ.get("PYDANTIC", "v2") != "v2":
    pytest.skip(
        "The test is skipped because we are running in limited Pydantic environment", allow_module_level=True
    )


def get_suspended_providers_folders() -> list[str]:
    """
    Returns a list of suspended providers folders that should be
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    suspended_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml["state"] == "suspended":
            suspended_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("airflow/providers/", "")
            )
    return suspended_providers


def get_python_excluded_providers_folders() -> list[str]:
    """
    Returns a list of providers folders that should be excluded for current Python version and
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    excluded_providers = []
    current_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    for provider_path in AIRFLOW_PROVIDERS_ROOT.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        excluded_python_versions = provider_yaml.get("excluded-python-versions", [])
        if current_python_version in excluded_python_versions:
            excluded_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("airflow/providers/", "")
            )
    return excluded_providers


def example_not_excluded_dags():
    example_dirs = ["airflow/**/example_dags/example_*.py", "tests/system/**/example_*.py"]
    excluded_providers_folders = get_suspended_providers_folders()
    excluded_providers_folders.extend(get_python_excluded_providers_folders())
    possible_prefixes = ["airflow/providers/", "tests/system/providers/"]
    suspended_providers_folders = [
        AIRFLOW_SOURCES_ROOT.joinpath(prefix, provider).as_posix()
        for prefix in possible_prefixes
        for provider in excluded_providers_folders
    ]
    for example_dir in example_dirs:
        candidates = glob(f"{AIRFLOW_SOURCES_ROOT.as_posix()}/{example_dir}", recursive=True)
        for candidate in candidates:
            if not candidate.startswith(tuple(suspended_providers_folders)):
                yield candidate


def example_dags_except_db_exception():
    return [
        dag_file
        for dag_file in example_not_excluded_dags()
        if not dag_file.endswith(tuple(NO_DB_QUERY_EXCEPTION))
    ]


def relative_path(path):
    return os.path.relpath(path, AIRFLOW_SOURCES_ROOT.as_posix())


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags(), ids=relative_path)
def test_should_be_importable(example):
    dagbag = DagBag(
        dag_folder=example,
        include_examples=False,
    )
    assert len(dagbag.import_errors) == 0, f"import_errors={str(dagbag.import_errors)}"
    assert len(dagbag.dag_ids) >= 1


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_dags_except_db_exception(), ids=relative_path)
def test_should_not_do_database_queries(example):
    with assert_queries_count(0, stacklevel_from_module=example.rsplit(os.sep, 1)[-1]):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
