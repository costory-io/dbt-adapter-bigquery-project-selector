from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbt.adapters.bigquery.credentials import BigQueryCredentials
from dbt_common.exceptions import DbtConfigError


@dataclass
class BigQueryCostOptimizationCredentials(BigQueryCredentials):
    execution_projects_by_tag: dict[str, str] | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self.execution_projects_by_tag = _normalize_projects_by_tag(
            self.execution_projects_by_tag
        )

    @property
    def type(self) -> str:
        return "bigquery_cost_optimization"

    def _connection_keys(self) -> tuple[str, ...]:
        return super()._connection_keys() + ("execution_projects_by_tag",)


def _normalize_projects_by_tag(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise DbtConfigError(
            "execution_projects_by_tag must be a mapping of dbt tag to GCP project id"
        )

    normalized: dict[str, str] = {}
    for tag, project in raw.items():
        tag_name = str(tag).strip()
        project_id = "" if project is None else str(project).strip()
        if not tag_name:
            raise DbtConfigError("execution_projects_by_tag contains an empty tag")
        if not project_id:
            label = tag_name or "<empty>"
            raise DbtConfigError(
                f"execution_projects_by_tag[{label}] must be a GCP project id"
            )
        normalized[tag_name] = project_id
    return normalized
