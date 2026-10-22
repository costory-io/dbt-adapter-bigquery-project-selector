from __future__ import annotations

from collections.abc import Mapping, Sequence

from dbt_common.exceptions import DbtRuntimeError


def resolve_execution_project(
    tags: Sequence[str],
    *,
    projects_by_tag: Mapping[str, str],
    node_name: str | None = None,
) -> tuple[str, str] | None:
    """Return ``(tag, project)`` when a node tag selects an execution project.

    No configured tag returns ``None`` so the caller keeps the client default.
    Tags that select different projects are an error. Tags that select the same
    project resolve to that project; the first matching tag is the one reported.
    """
    matches = [(tag, projects_by_tag[tag]) for tag in tags if tag in projects_by_tag]
    if not matches:
        return None

    projects = {project for _, project in matches}
    if len(projects) > 1:
        rendered = ", ".join(f"{tag} -> {project}" for tag, project in matches)
        node = node_name or "<unknown>"
        raise DbtRuntimeError(
            f"Node {node} matches multiple execution projects: {rendered}"
        )

    return matches[0]
