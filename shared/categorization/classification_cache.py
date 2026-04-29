from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def product_fingerprint(product: dict[str, Any]) -> str:
    raw = "|".join(
        [
            str(product.get("name", "")).strip().lower(),
            str(product.get("description", ""))[:500].strip().lower(),
            str(product.get("brandName", "")).strip().lower(),
            str(product.get("sku", "")).strip().lower(),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    try:
        with path.open(encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    items = data.get("items")
    if not isinstance(items, dict):
        return {}
    return {str(key): value for key, value in items.items() if isinstance(value, dict)}


def save_cache(path: Path, items: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump({"items": items}, file, ensure_ascii=False, indent=2)
