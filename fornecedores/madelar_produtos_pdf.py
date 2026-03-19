"""
Madelar — relatório PDF (produtos / tabela).

Extrai tabela do PDF com pdfplumber (extract_table / extract_tables).
Sempre devolve DataFrame com colunas padronizadas para o mapper e o banco.
"""

from __future__ import annotations

import io
import re
from typing import BinaryIO

import pandas as pd
import pdfplumber

# Colunas esperadas pelo mapper (_madelar_to_rows em db/mappers.py)
COLUNAS = [
    "Filial",
    "Código",
    "Descrição do Item",
    "Dt. Compra",
    "P. Custo",
    "Compras",
    "Estoque",
    "Vendida",
    "Preço Médio",
    "Méd. Venda Últ. 3m",
    "Tipo Item",
]

TABLE_SETTINGS = {
    "vertical_strategy": "text",
    "horizontal_strategy": "text",
    "snap_tolerance": 3,
}


def _celulas_linha(linha: list | None, n: int) -> list[str]:
    if not linha:
        return [""] * n
    out = [
        str(c).replace("\n", " ").strip() if c is not None else "" for c in linha
    ]
    while len(out) < n:
        out.append("")
    return out[:n]


def _linha_eh_cabecalho_ou_vazia(linha_limpa: list[str]) -> bool:
    if not any(s and str(s).strip() for s in linha_limpa):
        return True
    first = (linha_limpa[0] or "").strip().lower()
    if first == "filial" or any("filial" in (s or "").lower() for s in linha_limpa[:3]):
        return True
    return False


def _normaliza_numero(celula: str) -> str:
    """Substitui vírgula por ponto para o mapper converter a float."""
    if not celula or not isinstance(celula, str):
        return celula or ""
    return re.sub(r",", ".", str(celula).strip())


def _extrai_tabelas_pagina(pagina) -> list[list[list]]:
    """Tenta extract_table e extract_tables; devolve lista de tabelas (listas de linhas)."""
    tabelas: list[list[list]] = []
    t = pagina.extract_table(TABLE_SETTINGS)
    if t:
        tabelas.append(t)
    if not tabelas:
        t = pagina.extract_table()
        if t:
            tabelas.append(t)
    if not tabelas:
        # extract_tables retorna lista de tabelas por página
        multi = pagina.extract_tables()
        if multi:
            tabelas.extend(multi)
    return tabelas


def parse_madelar_produtos_pdf(file: BinaryIO | bytes) -> pd.DataFrame:
    """
    Lê o PDF Madelar (bytes ou upload) e devolve DataFrame com as colunas da tabela.
    Sempre retorna um DataFrame com as colunas COLUNAS (vazio ou com dados).
    """
    raw = file if isinstance(file, bytes) else file.read()
    if not raw:
        return pd.DataFrame(columns=COLUNAS)
    buffer = io.BytesIO(raw)
    buffer.seek(0)
    n = len(COLUNAS)
    dados: list[list[str]] = []

    try:
        with pdfplumber.open(buffer) as pdf:
            for pagina in pdf.pages:
                for tabela in _extrai_tabelas_pagina(pagina):
                    if not tabela:
                        continue
                    for linha in tabela:
                        linha_limpa = _celulas_linha(linha, n)
                        if _linha_eh_cabecalho_ou_vazia(linha_limpa):
                            continue
                        # Normalizar campos numéricos (vírgula -> ponto)
                        for i in (4, 5, 6, 7, 8, 9):  # P. Custo, Compras, Estoque, Vendida, Preço Médio, Méd. Venda
                            if i < len(linha_limpa) and linha_limpa[i]:
                                linha_limpa[i] = _normaliza_numero(linha_limpa[i])
                        dados.append(linha_limpa)
    except Exception:
        return pd.DataFrame(columns=COLUNAS)

    if not dados:
        return pd.DataFrame(columns=COLUNAS)

    df = pd.DataFrame(dados)
    if df.shape[1] < n:
        for i in range(df.shape[1], n):
            df[i] = ""
    elif df.shape[1] > n:
        df = df.iloc[:, :n].copy()
    df.columns = list(COLUNAS)
    df = df.reset_index(drop=True)
    return df
