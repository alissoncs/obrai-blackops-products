from __future__ import annotations

from typing import Any


def build_product_context(product: dict[str, Any]) -> str:
    name = str(product.get("name", "")).strip()
    description = str(product.get("description", "")).strip()[:500]
    brand = str(product.get("brandName", "")).strip()
    sku = str(product.get("sku", "")).strip()
    return f"nome={name}\nmarca={brand}\nsku={sku}\ndescricao={description}"


def is_ambiguous_product(product: dict[str, Any]) -> bool:
    description = str(product.get("description", "")).strip()
    name = str(product.get("name", "")).strip()
    if len(description) < 40:
        return True
    if len(name.split()) <= 2:
        return True
    return False
