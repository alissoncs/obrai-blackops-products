"""
Obraí BlackOps — home. Navegue pelo menu lateral.
Execute: streamlit run app.py
"""

from __future__ import annotations

import streamlit as st

from layout import sidebar

st.set_page_config(
    page_title="Obraí BlackOps",
    layout="wide",
    initial_sidebar_state="expanded",
)
sidebar()

st.title("Obraí BlackOps")
st.caption("Importação de produtos para o marketplace.")

st.markdown("""
Use o **menu lateral** para:

- **Importar** — escolher o parser, anexar o arquivo e (opcional) salvar no SQLite.
- **Importações** — ver a tabela de importações salvas, detalhes e produtos.
""")
