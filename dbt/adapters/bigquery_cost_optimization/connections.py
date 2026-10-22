from __future__ import annotations

from typing import Any

from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.bigquery_cost_optimization.node_context import (
    clear_current_node,
    get_current_node,
    node_name_from,
    node_tags_from,
    set_current_node,
)
from dbt.adapters.bigquery_cost_optimization.query_routing import (
    resolve_execution_project,
)

logger = AdapterLogger("BigQueryCostOptimization")


class _NodeCapturingQueryHeader:
    """Remember the dbt node passed into the query-comment header for this thread."""

    def __init__(self, inner: Any):
        self._inner = inner

    def set(self, name: str, query_header_context: Any) -> None:
        set_current_node(query_header_context)
        self._inner.set(name, query_header_context)

    def reset(self) -> None:
        clear_current_node()
        self._inner.reset()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class ExecutionProjectQueryClient:
    """Thin wrapper that injects ``project`` into ``Client.query`` calls."""

    def __init__(self, client: Any, execution_project: str):
        self._client = client
        self._execution_project = execution_project

    def query(self, *args: Any, **kwargs: Any):
        kwargs["project"] = self._execution_project
        return self._client.query(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class BigQueryCostOptimizationConnectionManager(BigQueryConnectionManager):
    TYPE = "bigquery_cost_optimization"

    def set_query_header(self, query_header_context: dict[str, Any]) -> None:
        clear_current_node()
        super().set_query_header(query_header_context)
        if self.query_header is not None and not isinstance(
            self.query_header, _NodeCapturingQueryHeader
        ):
            self.query_header = _NodeCapturingQueryHeader(self.query_header)

    def _query_and_results(
        self,
        conn,
        sql,
        job_params,
        job_id,
        limit: int | None = None,
    ):
        node = get_current_node()
        node_name = node_name_from(node)
        credentials = conn.credentials
        projects_by_tag = credentials.execution_projects_by_tag or {}
        route = resolve_execution_project(
            node_tags_from(node),
            projects_by_tag=projects_by_tag,
            node_name=node_name,
        )

        if route is None:
            return super()._query_and_results(
                conn, sql, job_params, job_id, limit=limit
            )

        tag, project = route
        logger.debug(
            f"Routing node_name={node_name or '<unknown>'} tag={tag} "
            f"execution_project={project}"
        )
        original_handle = conn.handle
        conn.handle = ExecutionProjectQueryClient(original_handle, project)
        try:
            return super()._query_and_results(
                conn, sql, job_params, job_id, limit=limit
            )
        finally:
            conn.handle = original_handle
