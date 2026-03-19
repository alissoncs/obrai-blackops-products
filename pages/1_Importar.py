"""
Página: Importar — parser + arquivo; importa e salva no banco diretamente.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from db import save_import
from layout import sidebar
from parsers_registry import PARSERS, get_parser

st.set_page_config(page_title="Importar — Obraí", layout="wide")
sidebar()

st.title("Importar")
st.caption("Escolha o parser e envie o arquivo. A importação é salva no banco automaticamente.")

parser_labels = {p["label"]: p["id"] for p in PARSERS}
label = st.selectbox("Parser", options=list(parser_labels.keys()))
parser_id = parser_labels[label]
info = get_parser(parser_id)
assert info is not None

if info.get("expected_format"):
    st.caption(f"**Formato esperado:** {info['expected_format']}")

nome_imp = st.text_input(
    "Nome da importação (opcional)",
    placeholder="Ex.: Flank semana 12/2025",
    key="save_import_name",
)

uploaded = st.file_uploader(
    "Arquivo",
    type=None,
    help="Arraste o arquivo ou clique para enviar. Será importado e salvo no banco.",
)

if uploaded is not None and info:
    try:
        data = info["parse"](uploaded.getvalue())
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame()
        imp_id = save_import(
            parser_id=parser_id,
            parser_label=label,
            df=data,
            source_filename=uploaded.name,
            nome=nome_imp.strip() or None,
        )
        st.success(f"Importação **#{imp_id}** gravada ({len(data)} produto(s)). Veja em **Importações** no menu.")
    except Exception as e:
        st.error(f"Erro: {e}")
