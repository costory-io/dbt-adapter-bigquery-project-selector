from __future__ import annotations

import threading
from typing import Any

_node_local = threading.local()


def set_current_node(node: Any) -> None:
    _node_local.node = node


def get_current_node() -> Any:
    return getattr(_node_local, "node", None)


def clear_current_node() -> None:
    _node_local.node = None


def node_name_from(node: Any) -> str | None:
    name = getattr(node, "name", None) if node is not None else None
    if name:
        return str(name)
    return None


def node_tags_from(node: Any) -> list[str]:
    if node is None:
        return []
    tags = getattr(node, "tags", None) or []
    return [str(tag) for tag in tags if tag is not None and str(tag) != ""]
