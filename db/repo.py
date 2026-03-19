"""
Operações no SQLite: importacoes e produtos.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from db.connection import get_connection, init_db
from db.mappers import dataframe_to_produto_rows

PRODUTOS_COLS = [
    "importacao_id", "row_index", "sku", "ean", "friendly_id", "slug", "name",
    "description", "rich_description", "main_image", "additional_images",
    "brand_id", "primary_category_id", "price_type_id", "tags",
    "supplier_branch_id", "retail_price", "wholesale_price",
    "minimum_wholesale_quantity", "supplier_description", "supplier_rich_description",
    "stock_quantity", "status", "created_at", "updated_at",
]


def save_import(
    parser_id: str,
    parser_label: str,
    df: pd.DataFrame,
    source_filename: str | None = None,
    nome: str | None = None,
) -> int:
    """Grava importação + produtos. Retorna importacao id."""
    if df is None or not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()
    init_db()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = dataframe_to_produto_rows(parser_id, df)
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO importacoes (
                created_at, nome, parser_id, parser_label, source_filename,
                product_count, submitted_to_obrai, submitted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)""",
            (
                now,
                (nome or "").strip() or None,
                parser_id,
                parser_label,
                source_filename or "",
                len(rows),
                now,
            ),
        )
        importacao_id = cur.lastrowid
        for r in rows:
            conn.execute(
                """INSERT INTO produtos (
                    importacao_id, row_index, sku, ean, friendly_id, slug, name,
                    description, rich_description, main_image, additional_images,
                    brand_id, primary_category_id, price_type_id, tags,
                    supplier_branch_id, retail_price, wholesale_price,
                    minimum_wholesale_quantity, supplier_description, supplier_rich_description,
                    stock_quantity, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    importacao_id,
                    r["row_index"],
                    r["sku"],
                    r["ean"],
                    r["friendly_id"],
                    r["slug"],
                    r["name"],
                    r["description"],
                    r["rich_description"],
                    r["main_image"],
                    r["additional_images"],
                    r["brand_id"],
                    r["primary_category_id"],
                    r["price_type_id"],
                    r["tags"],
                    r["supplier_branch_id"],
                    r["retail_price"],
                    r["wholesale_price"],
                    r["minimum_wholesale_quantity"],
                    r["supplier_description"],
                    r["supplier_rich_description"],
                    r["stock_quantity"],
                    r["status"],
                    now,
                    now,
                ),
            )
        conn.commit()
        return importacao_id
    finally:
        conn.close()


def get_import(importacao_id: int) -> dict[str, Any] | None:
    """Retorna importação + lista de produtos (dicts)."""
    init_db()
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM importacoes WHERE id = ?", (importacao_id,)
        ).fetchone()
        if not row:
            return None
        imp = dict(row)
        imp["submitted_to_obrai"] = bool(imp.get("submitted_to_obrai"))
        rows = conn.execute(
            "SELECT * FROM produtos WHERE importacao_id = ? ORDER BY row_index",
            (importacao_id,),
        ).fetchall()
        imp["produtos"] = [dict(r) for r in rows]
        return imp
    finally:
        conn.close()


def list_imports(limit: int = 100) -> list[dict[str, Any]]:
    """Lista importações (mais recentes primeiro)."""
    init_db()
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id, created_at, nome, parser_id, parser_label, source_filename,
                      product_count, submitted_to_obrai, submitted_at
               FROM importacoes ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["submitted_to_obrai"] = bool(d.get("submitted_to_obrai"))
            out.append(d)
        return out
    finally:
        conn.close()


def delete_import(importacao_id: int) -> bool:
    """Remove importação e produtos (CASCADE). Retorna True se removeu."""
    init_db()
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM importacoes WHERE id = ?", (importacao_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def _parse_primary_category_id(v: Any) -> str | None:
    """Converte valor do dropdown (ex: 'Construção (1)') para id a gravar (ex: '1')."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r".*\(([^)]+)\)\s*$", s)
    return m.group(1).strip() if m else s


def update_produtos(importacao_id: int, df: pd.DataFrame) -> None:
    """Atualiza os produtos da importação no SQLite a partir do DataFrame editado."""
    if df is None or df.empty or "id" not in df.columns:
        return
    init_db()
    conn = get_connection()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cols_update = [
        "row_index", "sku", "ean", "friendly_id", "slug", "name",
        "description", "rich_description", "main_image", "additional_images",
        "brand_id", "primary_category_id", "price_type_id", "tags",
        "supplier_branch_id", "retail_price", "wholesale_price",
        "minimum_wholesale_quantity", "supplier_description", "supplier_rich_description",
        "stock_quantity", "status", "updated_at",
    ]
    try:
        for _, row in df.iterrows():
            prod_id = row.get("id")
            if pd.isna(prod_id) or prod_id is None:
                continue
            prod_id = int(prod_id)
            vals = []
            for c in cols_update:
                v = row.get(c)
                if c == "updated_at":
                    vals.append(now)
                elif c == "primary_category_id":
                    vals.append(_parse_primary_category_id(v))
                elif pd.isna(v) or v is None:
                    vals.append(None)
                elif c in ("row_index", "minimum_wholesale_quantity", "stock_quantity"):
                    try:
                        vals.append(int(float(v)))
                    except (TypeError, ValueError):
                        vals.append(None)
                elif c in ("retail_price", "wholesale_price"):
                    try:
                        vals.append(float(v))
                    except (TypeError, ValueError):
                        vals.append(None)
                else:
                    vals.append(str(v) if v is not None else None)
            placeholders = ", ".join(f"{c} = ?" for c in cols_update)
            conn.execute(
                f"UPDATE produtos SET {placeholders} WHERE id = ? AND importacao_id = ?",
                (*vals, prod_id, importacao_id),
            )
        conn.execute(
            "UPDATE importacoes SET updated_at = ? WHERE id = ?",
            (now, importacao_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_import_submitted(importacao_id: int, submitted: bool) -> bool:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if submitted else None
    init_db()
    conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE importacoes SET submitted_to_obrai = ?, submitted_at = ? WHERE id = ?",
            (1 if submitted else 0, now, importacao_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
