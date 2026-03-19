"""
Converte saída do parser (DataFrame) em linhas para a tabela produtos
(campos Product + SupplierProduct do Prisma).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd


def _slug(s: str, max_len: int = 80) -> str:
    s = unicodedata.normalize("NFKD", str(s).strip().lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[-\s]+", "-", s).strip("-")[:max_len]
    return s or "item"


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int_val(v: Any, default: int = 0) -> int:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def dataframe_to_produto_rows(parser_id: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    """Retorna lista de dicts prontos para INSERT em produtos (sem id, importacao_id, created_at, updated_at)."""
    if parser_id == "flank_materiais_csv":
        return _flank_to_rows(df)
    if parser_id == "madelar_produtos_pdf":
        return _madelar_to_rows(df)
    return _generic_to_rows(df)


def _row_template(row_index: int) -> dict[str, Any]:
    return {
        "row_index": row_index,
        "sku": None,
        "ean": None,
        "friendly_id": None,
        "slug": None,
        "name": "",
        "description": None,
        "rich_description": None,
        "main_image": None,
        "additional_images": None,
        "brand_id": None,
        "primary_category_id": None,
        "price_type_id": None,
        "tags": None,
        "supplier_branch_id": None,
        "retail_price": None,
        "wholesale_price": None,
        "minimum_wholesale_quantity": None,
        "supplier_description": None,
        "supplier_rich_description": None,
        "stock_quantity": 0,
        "status": "active",
    }


def _flank_to_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    seen: dict[str, int] = {}
    rows = []
    for idx, row in df.iterrows():
        nome = str(row.get("nome_produto", "") or "").strip()
        base = _slug(nome)
        n = seen.get(base, 0)
        seen[base] = n + 1
        sku = f"{base}-{n}" if n else base
        if len(sku) < 2:
            sku = f"{sku}{idx}"
        sku = sku[:64]
        r = _row_template(int(idx) if isinstance(idx, int) else len(rows))
        r["sku"] = sku
        r["slug"] = base[:80] if base else None
        r["name"] = nome or "(sem nome)"
        r["retail_price"] = _num(row.get("preco"))
        r["stock_quantity"] = _int_val(row.get("estoque"), 0)
        rows.append(r)
    return rows


def _madelar_get(row: pd.Series, *keys: str) -> Any:
    """Retorna o primeiro valor não vazio para uma das chaves (mapper Madelar)."""
    for k in keys:
        v = row.get(k)
        if v is not None and pd.notna(v) and str(v).strip() != "":
            return v
    return None


def _madelar_to_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    rows = []
    for idx, row in df.iterrows():
        cod = str(_madelar_get(row, "Código") or "").strip()
        sku = (cod or f"madelar-{idx}")[:128]
        nome = str(_madelar_get(row, "Descrição do Item", "Descricao do Item") or "").strip()
        filial = str(_madelar_get(row, "Filial") or "")
        tipo = str(_madelar_get(row, "Tipo Item", "Tipo_Item") or "")
        extras = [x for x in [f"Filial: {filial}" if filial else "", f"Tipo: {tipo}" if tipo else ""] if x]
        desc_sup = " | ".join(extras) or None
        r = _row_template(int(idx) if isinstance(idx, int) else len(rows))
        r["sku"] = sku
        r["slug"] = _slug(nome)[:80] if nome else None
        r["name"] = nome or "(sem nome)"
        r["retail_price"] = _num(_madelar_get(row, "Preço Médio", "Preco Medio", "P. Custo"))
        r["wholesale_price"] = _num(_madelar_get(row, "Méd. Venda Últ. 3m", "Méd_Venda_3m", "Méd. Venda Últ. 3m"))
        r["stock_quantity"] = _int_val(_madelar_get(row, "Estoque"), 0)
        r["supplier_description"] = desc_sup
        rows.append(r)
    return rows


def _generic_to_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = {str(c).lower().replace(" ", "_"): c for c in df.columns}
    def col(*names):
        for n in names:
            k = n.lower().replace(" ", "_")
            if k in cols:
                return cols[k]
        return None
    c_name = col("name", "nome", "nome_produto", "descrição", "descricao")
    c_sku = col("sku", "codigo", "código")
    c_price = col("retail_price", "preco", "preço", "price")
    c_stock = col("stock_quantity", "estoque", "stock", "qtd")
    rows = []
    for idx, row in df.iterrows():
        nome = str(row[c_name]) if c_name and pd.notna(row.get(c_name)) else f"Produto {idx}"
        sku = str(row[c_sku]).strip() if c_sku and pd.notna(row.get(c_sku)) else _slug(nome) + f"-{idx}"
        r = _row_template(int(idx) if isinstance(idx, int) else len(rows))
        r["sku"] = sku[:128]
        r["slug"] = _slug(nome)[:80]
        r["name"] = nome
        r["retail_price"] = _num(row[c_price]) if c_price else None
        r["stock_quantity"] = _int_val(row[c_stock], 0) if c_stock else 0
        rows.append(r)
    return rows
