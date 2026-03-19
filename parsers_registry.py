"""
Registro de parsers para a UI (dropdown).
Cada entrada: id estável, rótulo, função parse(file) -> pd.DataFrame.
"""

from __future__ import annotations

from typing import Any, BinaryIO, Callable

import pandas as pd

from fornecedores.flank_materiais_csv import parse_flank_csv
from fornecedores.madelar_produtos_pdf import parse_madelar_produtos_pdf

ParseFn = Callable[[BinaryIO | bytes], pd.DataFrame]

PARSERS: list[dict[str, Any]] = [
    {
        "id": "flank_materiais_csv",
        "label": "Flank Materiais de construção (CSV)",
        "parse": parse_flank_csv,
        # Formato esperado pelo parser (a UI aceita qualquer extensão no upload)
        "expected_format": "CSV (nome, estoque, preço)",
    },
    {
        "id": "madelar_produtos_pdf",
        "label": "Madelar Produtos Mais Vendidos PDF",
        "parse": parse_madelar_produtos_pdf,
        "expected_format": "PDF Madelar (tabela extraída — relatório padrão)",
    },
]


def get_parser(parser_id: str) -> dict[str, Any] | None:
    for p in PARSERS:
        if p["id"] == parser_id:
            return p
    return None
