"""
Flank Materiais de construção — CSV semanal.

Colunas esperadas: nome do produto, estoque, preço.
Aceita variações comuns de cabeçalho e formato numérico brasileiro.
"""

from __future__ import annotations

import io
import re
import unicodedata
from typing import BinaryIO

import pandas as pd


def _norm_header(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s).strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().replace(" ", "_")


def _find_column(df: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    for col in df.columns:
        key = _norm_header(col)
        for a in aliases:
            if key == a or key.replace("_", "") == a.replace("_", ""):
                return col
    return None


def _parse_preco(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)) and not pd.isna(val):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    s = re.sub(r"R\$\s?", "", s, flags=re.I).strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _parse_estoque(val) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip()
    if not s:
        return None
    s = re.sub(r"[^\d\-]", "", s)
    try:
        return int(s) if s else None
    except ValueError:
        return None


def parse_flank_csv(file: BinaryIO | bytes) -> pd.DataFrame:
    """
    Lê o CSV da Flank e devolve DataFrame normalizado:
    nome_produto, estoque, preco.
    """
    if isinstance(file, bytes):
        raw = file
    else:
        raw = file.read()

    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        df = pd.read_csv(io.BytesIO(raw), encoding="utf-8", errors="replace")

    df.columns = [str(c).strip() for c in df.columns]

    col_nome = _find_column(
        df,
        (
            "nome_do_produto",
            "nome_produto",
            "produto",
            "nome",
            "descricao",
            "descrição",
        ),
    )
    col_estoque = _find_column(df, ("estoque", "qtd", "quantidade", "qty"))
    col_preco = _find_column(df, ("preco", "preço", "price", "valor"))

    missing = []
    if not col_nome:
        missing.append("nome do produto")
    if not col_estoque:
        missing.append("estoque")
    if not col_preco:
        missing.append("preço")
    if missing:
        raise ValueError(
            "Colunas obrigatórias não encontradas: "
            + ", ".join(missing)
            + f". Cabeçalhos lidos: {list(df.columns)}"
        )

    out = pd.DataFrame(
        {
            "nome_produto": df[col_nome].astype(str).str.strip(),
            "estoque": df[col_estoque].map(_parse_estoque),
            "preco": df[col_preco].map(_parse_preco),
        }
    )
    out = out[out["nome_produto"].str.len() > 0]
    out = out.reset_index(drop=True)
    return out
