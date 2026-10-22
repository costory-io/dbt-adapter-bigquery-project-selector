"""Run a fake dbt project through dbt, with BigQuery replaced by an in-process client.

No GCP credentials or network calls. The client records each submitted job so the
test can check which execution project dbt actually selected.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from dbt.adapters.factory import reset_adapters
from dbt.cli.main import dbtRunner
from google.api_core.exceptions import NotFound

DEFAULT_PROJECT = "analytics-prod"
RESERVATION_PROJECT = "analytics-prod-reservation"
BACKFILL_PROJECT = "analytics-prod-backfill"

_PROJECT_YML = """\
name: analytics
profile: analytics
version: "1.0.0"
config-version: 2
flags:
  send_anonymous_usage_stats: false
models:
  analytics:
    +materialized: view
"""

_PROFILES_YML = f"""\
analytics:
  target: dev
  outputs:
    dev:
      type: bigquery_cost_optimization
      method: oauth
      project: {DEFAULT_PROJECT}
      execution_project: {DEFAULT_PROJECT}
      dataset: dbt
      location: EU
      threads: 1
      execution_projects_by_tag:
        reservation: {RESERVATION_PROJECT}
        slots: {RESERVATION_PROJECT}
        backfill: {BACKFILL_PROJECT}
"""

_MODELS = {
    "daily_usage.sql": """\
{{ config(tags=["daily"]) }}
select 1 as id
""",
    "reserved_usage.sql": """\
{{ config(tags=["reservation"]) }}
select 1 as id
""",
    "shared_usage.sql": """\
{{ config(tags=["reservation", "slots"]) }}
select 1 as id
""",
    "ambiguous_usage.sql": """\
{{ config(tags=["reservation", "backfill"]) }}
select 1 as id
""",
}


@dataclass
class SubmittedQuery:
    sql: str
    project: str | None
    labels: dict[str, str]


class FakeBigQueryClient:
    """Enough of the BigQuery client for a view materialization to finish."""

    def __init__(self) -> None:
        self.queries: list[SubmittedQuery] = []

    def query(self, *args: Any, **kwargs: Any) -> _QueryJob:
        sql = kwargs.get("query", args[0] if args else "")
        job_config = kwargs.get("job_config")
        labels = dict(getattr(job_config, "labels", None) or {})
        self.queries.append(
            SubmittedQuery(
                sql=sql,
                project=kwargs.get("project"),
                labels=labels,
            )
        )
        statement_type = "CREATE_VIEW" if " view " in f" {sql.lower()} " else "SCRIPT"
        return _QueryJob(statement_type)

    def list_datasets(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def list_tables(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def list_routines(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def get_table(self, *args: Any, **kwargs: Any) -> Any:
        raise NotFound("fake client has no tables")

    def get_routine(self, *args: Any, **kwargs: Any) -> Any:
        raise NotFound("fake client has no routines")

    def close(self) -> None:
        return None


class _QueryJob:
    def __init__(self, statement_type: str) -> None:
        self.location = None
        self.job_id = "fake-job"
        self.project = None
        self.statement_type = statement_type
        self.total_bytes_processed = 0
        self.total_bytes_billed = 0
        self.slot_millis = 0
        self.destination = None
        self.num_dml_affected_rows = None

    def result(self, **kwargs: Any) -> _QueryJob:
        return self

    def cancel(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _isolate_dbt_adapters():
    reset_adapters()
    yield
    reset_adapters()


def _write_project(root: Path) -> None:
    root.joinpath("dbt_project.yml").write_text(_PROJECT_YML)
    root.joinpath("profiles.yml").write_text(_PROFILES_YML)
    models = root / "models"
    models.mkdir()
    for name, sql in _MODELS.items():
        models.joinpath(name).write_text(sql)


def _run_dbt(root: Path, client: FakeBigQueryClient, command: str, *extra: str):
    args = [
        command,
        "--project-dir",
        str(root),
        "--profiles-dir",
        str(root),
        "--log-level",
        "error",
        *extra,
    ]
    with patch(
        "dbt.adapters.bigquery.connections.create_bigquery_client",
        return_value=client,
    ):
        return dbtRunner().invoke(args)


def _failure_text(invocation) -> str:
    parts: list[str] = []
    if invocation.exception is not None:
        parts.append(f"{type(invocation.exception).__name__}: {invocation.exception}")
    result = invocation.result
    results = getattr(result, "results", None)
    if results:
        for item in results:
            parts.append(
                f"{getattr(item, 'status', '')}: {getattr(item, 'message', '')}"
            )
    elif result is not None:
        parts.append(str(result))
    return "\n".join(parts)


def _queries_for(client: FakeBigQueryClient, model_name: str) -> list[SubmittedQuery]:
    return [query for query in client.queries if model_name in query.sql]


def test_dbt_run_routes_models_by_tag(tmp_path: Path):
    client = FakeBigQueryClient()
    invocation = _run_dbt(
        _project(tmp_path),
        client,
        "run",
        "--exclude",
        "ambiguous_usage",
    )

    assert invocation.success, _failure_text(invocation)

    daily = _queries_for(client, "daily_usage")
    reserved = _queries_for(client, "reserved_usage")
    shared = _queries_for(client, "shared_usage")
    assert len(daily) == 1
    assert len(reserved) == 1
    assert len(shared) == 1

    assert daily[0].project is None
    assert reserved[0].project == RESERVATION_PROJECT
    assert shared[0].project == RESERVATION_PROJECT

    for query, model_name in (
        (daily[0], "daily_usage"),
        (reserved[0], "reserved_usage"),
        (shared[0], "shared_usage"),
    ):
        assert f"`{DEFAULT_PROJECT}`.`dbt`.`{model_name}`" in query.sql
        assert "dbt_invocation_id" in query.labels

    schema_queries = [
        query
        for query in client.queries
        if "daily_usage" not in query.sql
        and "reserved_usage" not in query.sql
        and "shared_usage" not in query.sql
    ]
    assert schema_queries
    assert all(query.project is None for query in schema_queries)


def test_dbt_run_fails_ambiguous_tags_before_submit(tmp_path: Path):
    client = FakeBigQueryClient()
    invocation = _run_dbt(
        _project(tmp_path),
        client,
        "run",
        "--select",
        "ambiguous_usage",
    )

    assert not invocation.success
    message = _failure_text(invocation)
    assert "ambiguous_usage" in message
    assert RESERVATION_PROJECT in message
    assert BACKFILL_PROJECT in message
    assert _queries_for(client, "ambiguous_usage") == []


def _project(tmp_path: Path) -> Path:
    root = tmp_path / "analytics"
    root.mkdir()
    _write_project(root)
    return root
