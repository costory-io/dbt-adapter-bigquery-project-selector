"""Unit tests for tag routing. No GCP calls."""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from dbt_common.exceptions import DbtConfigError, DbtRuntimeError

from dbt.adapters.bigquery_cost_optimization.connections import (
    BigQueryCostOptimizationConnectionManager,
    _NodeCapturingQueryHeader,
)
from dbt.adapters.bigquery_cost_optimization.credentials import (
    BigQueryCostOptimizationCredentials,
)
from dbt.adapters.bigquery_cost_optimization.node_context import (
    clear_current_node,
    set_current_node,
)
from dbt.adapters.bigquery_cost_optimization.query_routing import (
    resolve_execution_project,
)

DEFAULT_PROJECT = "analytics-prod"
RESERVATION_PROJECT = "analytics-prod-reservation"
BACKFILL_PROJECT = "analytics-prod-backfill"


def _credentials(**overrides) -> BigQueryCostOptimizationCredentials:
    values = {
        "method": "oauth",
        "database": DEFAULT_PROJECT,
        "schema": "dbt",
        "execution_project": DEFAULT_PROJECT,
        "execution_projects_by_tag": {
            "reservation": RESERVATION_PROJECT,
            "backfill": BACKFILL_PROJECT,
        },
    }
    values.update(overrides)
    return BigQueryCostOptimizationCredentials(**values)


def _node(name: str = "usage_metrics", tags: list[str] | None = None):
    return SimpleNamespace(name=name, tags=list(tags or []))


def _make_manager() -> BigQueryCostOptimizationConnectionManager:
    manager = BigQueryCostOptimizationConnectionManager.__new__(
        BigQueryCostOptimizationConnectionManager
    )
    retry = MagicMock()
    retry.create_job_execution_timeout.return_value = 300
    retry.create_job_creation_timeout.return_value = 30
    retry.create_retry.return_value = MagicMock()
    manager._retry = retry
    return manager


def _make_conn(*, client=None, credentials=None):
    conn = MagicMock()
    conn.handle = client if client is not None else MagicMock()
    conn.credentials = credentials if credentials is not None else _credentials()
    return conn


def _stub_query_job(client: MagicMock) -> MagicMock:
    query_job = MagicMock()
    query_job.location = None
    query_job.job_id = "job-1"
    query_job.project = None
    query_job.result.return_value = MagicMock(name="row_iterator")
    client.query.return_value = query_job
    return query_job


class TestResolveExecutionProject(unittest.TestCase):
    def test_no_matching_tag_keeps_the_client_default(self):
        self.assertIsNone(
            resolve_execution_project(
                ["daily"],
                projects_by_tag={"reservation": RESERVATION_PROJECT},
                node_name="usage_metrics",
            )
        )

    def test_matching_tag_returns_that_project(self):
        self.assertEqual(
            resolve_execution_project(
                ["daily", "reservation"],
                projects_by_tag={"reservation": RESERVATION_PROJECT},
                node_name="usage_metrics",
            ),
            ("reservation", RESERVATION_PROJECT),
        )

    def test_tags_that_select_different_projects_fail(self):
        with self.assertRaises(DbtRuntimeError) as ctx:
            resolve_execution_project(
                ["reservation", "backfill"],
                projects_by_tag={
                    "reservation": RESERVATION_PROJECT,
                    "backfill": BACKFILL_PROJECT,
                },
                node_name="usage_metrics",
            )
        message = str(ctx.exception)
        self.assertIn("usage_metrics", message)
        self.assertIn("reservation", message)
        self.assertIn("backfill", message)

    def test_tags_that_select_the_same_project_succeed(self):
        tag, project = resolve_execution_project(
            ["reservation", "slots"],
            projects_by_tag={
                "reservation": RESERVATION_PROJECT,
                "slots": RESERVATION_PROJECT,
            },
            node_name="usage_metrics",
        )
        self.assertEqual(project, RESERVATION_PROJECT)
        self.assertEqual(tag, "reservation")


class TestCredentials(unittest.TestCase):
    def test_type_matches_the_adapter_module(self):
        self.assertEqual(_credentials().type, "bigquery_cost_optimization")

    def test_omitted_map_is_empty(self):
        credentials = _credentials(execution_projects_by_tag=None)
        self.assertEqual(credentials.execution_projects_by_tag, {})

    def test_blank_project_fails_validation(self):
        with self.assertRaises(DbtConfigError):
            _credentials(execution_projects_by_tag={"reservation": "  "})

    def test_non_mapping_fails_validation(self):
        with self.assertRaises(DbtConfigError):
            _credentials(execution_projects_by_tag=["reservation"])

    def test_values_are_stripped(self):
        credentials = _credentials(
            execution_projects_by_tag={" reservation ": f"  {RESERVATION_PROJECT}  "}
        )
        self.assertEqual(
            credentials.execution_projects_by_tag,
            {"reservation": RESERVATION_PROJECT},
        )


class TestQuerySubmission(unittest.TestCase):
    def tearDown(self):
        clear_current_node()

    def test_matching_tag_patches_the_execution_project(self):
        manager = _make_manager()
        client = MagicMock()
        _stub_query_job(client)
        conn = _make_conn(client=client)
        set_current_node(_node(tags=["reservation", "daily"]))

        manager._query_and_results(
            conn,
            "select 1",
            {"use_legacy_sql": False, "labels": {"app": "dbt"}, "dry_run": False},
            job_id="job-abc",
        )

        call_kwargs = client.query.call_args.kwargs
        self.assertEqual(call_kwargs["project"], RESERVATION_PROJECT)
        self.assertEqual(call_kwargs["query"], "select 1")
        self.assertEqual(call_kwargs["job_id"], "job-abc")
        self.assertIs(conn.handle, client)

    def test_unconfigured_tag_does_not_patch_the_project(self):
        manager = _make_manager()
        client = MagicMock()
        _stub_query_job(client)
        conn = _make_conn(client=client)
        set_current_node(_node(tags=["daily"]))

        manager._query_and_results(
            conn,
            "select 1",
            {"use_legacy_sql": False, "labels": {}, "dry_run": False},
            job_id="job-od",
        )

        self.assertNotIn("project", client.query.call_args.kwargs)
        self.assertIs(conn.handle, client)

    def test_job_labels_from_the_query_comment_are_passed_through(self):
        manager = _make_manager()
        client = MagicMock()
        _stub_query_job(client)
        conn = _make_conn(client=client)
        set_current_node(_node(tags=["reservation"]))
        labels = {"app": "dbt", "node_name": "usage_metrics", "domain": "ACME Company"}

        manager._query_and_results(
            conn,
            "select 1",
            {"labels": dict(labels)},
            job_id="job-labels",
        )

        self.assertEqual(client.query.call_args.kwargs["job_config"].labels, labels)

    def test_handle_is_restored_when_the_query_fails(self):
        manager = _make_manager()
        client = MagicMock()
        client.query.side_effect = RuntimeError("boom")
        conn = _make_conn(client=client)
        set_current_node(_node(tags=["reservation"]))

        with self.assertRaises(RuntimeError):
            manager._query_and_results(
                conn,
                "select 1",
                {"labels": {}},
                job_id="job-err",
            )

        self.assertIs(conn.handle, client)

    def test_ambiguous_tags_fail_before_the_query_is_submitted(self):
        manager = _make_manager()
        client = MagicMock()
        conn = _make_conn(client=client)
        set_current_node(_node(tags=["reservation", "backfill"]))

        with self.assertRaises(DbtRuntimeError):
            manager._query_and_results(
                conn,
                "select 1",
                {"labels": {}},
                job_id="job-ambiguous",
            )

        client.query.assert_not_called()
        self.assertIs(conn.handle, client)

    def test_concurrent_nodes_do_not_share_a_route(self):
        manager = _make_manager()
        barrier = threading.Barrier(2)
        results: dict[str, str | None] = {}
        errors: list[BaseException] = []

        def run_query(thread_key: str, tags: list[str], expected_project: str | None):
            client = MagicMock()
            _stub_query_job(client)
            conn = _make_conn(client=client)
            set_current_node(_node(name=thread_key, tags=tags))
            try:
                barrier.wait(timeout=5)
                manager._query_and_results(
                    conn,
                    f"select '{thread_key}'",
                    {"labels": {}},
                    job_id=f"job-{thread_key}",
                )
                project = client.query.call_args.kwargs.get("project")
                results[thread_key] = project
                if project != expected_project:
                    raise AssertionError(f"{thread_key}: project={project!r}")
            except BaseException as exc:  # noqa: BLE001 - report from the main thread
                errors.append(exc)
            finally:
                clear_current_node()

        threads = [
            threading.Thread(
                target=run_query,
                args=("daily_model", ["daily"], None),
            ),
            threading.Thread(
                target=run_query,
                args=("heavy_model", ["reservation"], RESERVATION_PROJECT),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(errors, f"concurrent routing errors: {errors}")
        self.assertIsNone(results["daily_model"])
        self.assertEqual(results["heavy_model"], RESERVATION_PROJECT)


class TestQueryHeaderCapture(unittest.TestCase):
    def tearDown(self):
        clear_current_node()

    def test_set_and_reset_follow_the_dbt_node_on_this_thread(self):
        inner = MagicMock()
        header = _NodeCapturingQueryHeader(inner)
        node = _node(tags=["reservation"])

        header.set("model.analytics.usage_metrics", node)
        from dbt.adapters.bigquery_cost_optimization.node_context import (
            get_current_node,
        )

        self.assertIs(get_current_node(), node)
        inner.set.assert_called_once_with("model.analytics.usage_metrics", node)

        header.reset()
        self.assertIsNone(get_current_node())
        inner.reset.assert_called_once_with()


class TestPlugin(unittest.TestCase):
    def test_adapter_type_matches_the_import_path(self):
        from dbt.adapters.bigquery_cost_optimization import Plugin

        self.assertEqual(Plugin.adapter.type(), "bigquery_cost_optimization")
        self.assertEqual(Plugin.dependencies, ["bigquery"])
