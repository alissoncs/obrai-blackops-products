#!/usr/bin/env python3
"""Junta tigre_categories.json a tigre_products.json: primaryCategoryId = categorySlug (string)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def main() -> None:
    p = argparse.ArgumentParser(description="Aplicar categorySlug ao primaryCategoryId dos produtos.")
    p.add_argument(
        "--categories",
        type=Path,
        default=SCRIPT_DIR / "output" / "tigre_categories.json",
        help="Saída do enriquecer_categorias.py",
    )
    p.add_argument(
        "--products",
        type=Path,
        default=SCRIPT_DIR / "output" / "tigre_products.json",
        help="JSON de produtos a atualizar",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Ficheiro de saída (default: mesmo que --products)",
    )
    args = p.parse_args()
    out_path = args.out or args.products

    with args.categories.open(encoding="utf-8") as f:
        cdata = json.load(f)
    mapping: dict[str, str] = {}
    for it in cdata.get("items") or []:
        if not isinstance(it, dict):
            continue
        slug = str(it.get("slug", "")).strip()
        cat = str(it.get("categorySlug", "")).strip()
        if slug and cat:
            mapping[slug] = cat

    with args.products.open(encoding="utf-8") as f:
        pdata = json.load(f)
    products = pdata.get("products")
    if not isinstance(products, list):
        raise SystemExit("tigre_products.json deve ter chave 'products'")
    updated = 0
    for prod in products:
        if not isinstance(prod, dict):
            continue
        slug = str(prod.get("slug", "")).strip()
        if slug in mapping:
            prod["primaryCategoryId"] = mapping[slug]
            updated += 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(pdata, f, ensure_ascii=False, indent=2)
    print(f"primaryCategoryId atualizado em {updated} produtos (mapeamento {len(mapping)} entradas) -> {out_path}")


if __name__ == "__main__":
    main()