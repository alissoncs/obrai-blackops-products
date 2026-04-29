from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class CategoryNode:
    id: str
    name: str
    children: list["CategoryNode"]


def _parse_node(raw: dict[str, Any]) -> CategoryNode | None:
    if raw.get("status") != "active":
        return None
    if raw.get("disabledAt") is not None:
        return None
    node_id = str(raw.get("id", "")).strip()
    name = str(raw.get("name", "")).strip()
    if not node_id or not name:
        return None
    children_raw = raw.get("children") or []
    children: list[CategoryNode] = []
    for child in children_raw:
        if not isinstance(child, dict):
            continue
        parsed = _parse_node(child)
        if parsed:
            children.append(parsed)
    return CategoryNode(id=node_id, name=name, children=children)


def fetch_categories_tree(endpoint: str, timeout_seconds: int = 60) -> list[CategoryNode]:
    response = requests.get(endpoint, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("API de categorias deve retornar array")
    tree: list[CategoryNode] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        parsed = _parse_node(item)
        if parsed:
            tree.append(parsed)
    return tree


def flatten_taxonomy_level3(tree: list[CategoryNode]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []

    def walk(node: CategoryNode, path: list[str]) -> None:
        next_path = path + [node.name]
        if len(next_path) == 3:
            out.append(
                {
                    "id": node.id,
                    "name": node.name,
                    "path": " > ".join(next_path),
                    "parent_id": path and node.id or "",
                }
            )
        for child in node.children:
            walk(child, next_path)

    for root in tree:
        walk(root, [])
    return out


def top_level_categories(tree: list[CategoryNode]) -> list[dict[str, str]]:
    return [{"id": node.id, "name": node.name} for node in tree]


def stage2_candidates_by_parent(tree: list[CategoryNode]) -> dict[str, list[dict[str, str]]]:
    mapping: dict[str, list[dict[str, str]]] = {}

    def walk(node: CategoryNode, path: list[str], top_parent_id: str | None) -> None:
        next_path = path + [node.name]
        parent = top_parent_id if top_parent_id else node.id
        if len(next_path) == 3:
            mapping.setdefault(parent, []).append(
                {"id": node.id, "name": node.name, "path": " > ".join(next_path)}
            )
            return
        for child in node.children:
            walk(child, next_path, parent)

    for root in tree:
        walk(root, [], None)
    return mapping
