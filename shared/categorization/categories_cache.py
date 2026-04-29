from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.categorization.categories_client import CategoryNode


def _serialize_tree(tree: list[CategoryNode]) -> list[dict[str, Any]]:
    def node_to_dict(node: CategoryNode) -> dict[str, Any]:
        return {
            "id": node.id,
            "name": node.name,
            "children": [node_to_dict(child) for child in node.children],
        }

    return [node_to_dict(node) for node in tree]


def save_taxonomy_snapshot(
    *,
    cache_dir: Path,
    source_url: str,
    tree: list[CategoryNode],
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = cache_dir / f"taxonomy_snapshot_{timestamp}.json"
    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_url": source_url,
        "tree": _serialize_tree(tree),
    }
    with path.open("w", encoding="utf-8") as file:
        json.dump(doc, file, ensure_ascii=False, indent=2)
    return path
