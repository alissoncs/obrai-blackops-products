from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

DEFAULT_CATEGORIES_URL = (
    "https://obrai-app1-ifnsz.ondigitalocean.app/api/catalog/categories"
    "?displayMainMenu=true"
)


@dataclass(frozen=True)
class CategoryLevel3:
    id: str
    name: str
    slug: str
    path: str
    level0_id: str
    level0_name: str
    level1_id: str
    level1_name: str


def _is_active(node: dict[str, Any]) -> bool:
    return node.get("status") == "active" and node.get("disabledAt") is None


def _name(node: dict[str, Any]) -> str:
    return str(node.get("name") or "").strip()


def _node_id(node: dict[str, Any]) -> str:
    return str(node.get("id") or "").strip()


def _slug(node: dict[str, Any]) -> str:
    return str(node.get("slug") or "").strip()


def fetch_categories_tree(url: str, timeout_s: float = 30.0) -> list[dict[str, Any]]:
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout_s) as resp:
        status = getattr(resp, "status", 200)
        if status < 200 or status >= 300:
            raise ValueError(f"Falha ao buscar categorias: HTTP {status}")
        payload = json.loads(resp.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError("API de categorias retornou formato inválido: esperado array")
    return payload


def cache_categories_snapshot(
    tree: list[dict[str, Any]],
    *,
    cache_dir: Path,
    source_url: str,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = cache_dir / f"categories_snapshot_{ts}.json"
    doc = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "sourceUrl": source_url,
        "countRootNodes": len(tree),
        "tree": tree,
    }
    out_path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def flatten_level3_categories(tree: list[dict[str, Any]]) -> list[CategoryLevel3]:
    level3: list[CategoryLevel3] = []

    for l0 in tree:
        if not isinstance(l0, dict) or not _is_active(l0):
            continue
        l0_children = l0.get("children") or []
        for l1 in l0_children:
            if not isinstance(l1, dict) or not _is_active(l1):
                continue
            l1_children = l1.get("children") or []
            for l2 in l1_children:
                if not isinstance(l2, dict) or not _is_active(l2):
                    continue
                cid = _node_id(l2)
                cname = _name(l2)
                cslug = _slug(l2)
                if not cid or not cname:
                    continue
                path = " > ".join([_name(l0), _name(l1), cname])
                level3.append(
                    CategoryLevel3(
                        id=cid,
                        name=cname,
                        slug=cslug,
                        path=path,
                        level0_id=_node_id(l0),
                        level0_name=_name(l0),
                        level1_id=_node_id(l1),
                        level1_name=_name(l1),
                    )
                )

    # Remove duplicates by id preserving first occurrence.
    dedup: dict[str, CategoryLevel3] = {}
    for item in level3:
        dedup.setdefault(item.id, item)
    return list(dedup.values())

