"""
Obraí BlackOps — importação: escolha o parser e envie o arquivo.
Execute: streamlit run app.py
"""

from __future__ import annotations

import streamlit as st

from parsers_registry import PARSERS, get_parser

st.set_page_config(page_title="Obraí — Importação", layout="centered")

st.title("Importação de produtos")
st.caption("Obraí BlackOps")

parser_labels = {p["label"]: p["id"] for p in PARSERS}
label = st.selectbox("Parser", options=list(parser_labels.keys()))
parser_id = parser_labels[label]
info = get_parser(parser_id)
assert info is not None

ext = ", ".join(info.get("file_types", ["*"]))
uploaded = st.file_uploader(
    "Arquivo",
    type=info.get("file_types"),
    help=f"Formatos aceitos para este parser: {ext}",
)

if uploaded is not None and info:
    try:
        data = info["parse"](uploaded.getvalue())
        st.success(f"{len(data)} produto(s) lidos.")
        st.dataframe(data, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Erro ao processar o arquivo: {e}")
