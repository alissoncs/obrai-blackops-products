"""
Sidebar compartilhado entre as páginas.
"""

from __future__ import annotations

import streamlit as st

from db import get_db_path


def sidebar() -> None:
    st.sidebar.markdown("### Obraí BlackOps")
    st.sidebar.caption("Importação de produtos")
    st.sidebar.markdown("---")
    st.sidebar.caption(f"SQLite: `{get_db_path().name}`")
