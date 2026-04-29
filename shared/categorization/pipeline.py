from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .categories_api import CategoryLevel3
from .openai_classifier import OpenAiClassifier


@dataclass
class RunCounters:
    loaded: int = 0
    queued: int = 0
    success: int = 0
    failed: int = 0


def load_products_json(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    products = payload.get("products") if isinstance(payload, dict) else None
    if not isinstance(products, list):
        raise ValueError("JSON inválido: esperado chave 'products' como array")
    return [p for p in products if isinstance(p, dict)]


def write_products_json(path: Path, products: list[dict[str, Any]]) -> None:
    doc = {"version": 1, "products": products}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")


def run_stage1_pipeline(
    *,
    products: list[dict[str, Any]],
    level3_categories: list[CategoryLevel3],
    classifier: OpenAiClassifier,
    limit: int | None,
    dry_run: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], RunCounters]:
    counters = RunCounters(loaded=len(products))
    output_products = [dict(p) for p in products]
    errors: list[dict[str, Any]] = []

    queue = output_products
    if limit is not None and limit >= 0:
        queue = output_products[:limit]
    counters.queued = len(queue)

    if dry_run:
        return output_products, errors, counters

    for prod in queue:
        slug = str(prod.get("slug") or "")
        try:
            decision = classifier.classify_product(
                product=prod,
                level3_categories=level3_categories,
            )
            prod["primaryCategoryId"] = decision.level3_id
            prod["primaryCategoryName"] = decision.level3_name
            prod["categoryPath"] = next(
                (c.path for c in level3_categories if c.id == decision.level3_id), ""
            )
            prod["categoryConfidence"] = decision.confidence
            prod["categoryReason"] = decision.reason
            prod["categoryUpdatedAt"] = datetime.now(timezone.utc).isoformat()
            counters.success += 1
        except Exception as exc:  # noqa: BLE001
            counters.failed += 1
            errors.append(
                {
                    "slug": slug,
                    "sku": str(prod.get("sku") or ""),
                    "error": str(exc),
                }
            )

    return output_products, errors, counters

