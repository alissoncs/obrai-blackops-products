"""
Página: Importações — lista ou detalhe (conforme query param ?id=).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from db import delete_import, get_import, list_imports, set_import_submitted, update_produtos
from layout import sidebar

st.set_page_config(page_title="Importações — Obraí", layout="wide")
sidebar()

# Detalhe: ?id=123 na URL (mesma página, outro conteúdo)
imp_id_param = st.query_params.get("id")
if imp_id_param:
    try:
        imp_id = int(imp_id_param)
    except ValueError:
        imp_id = None
else:
    imp_id = None

def _formata_atualizado(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return iso[:19].replace("T", " ") if iso else "—"


def _df_alterado(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if a.shape != b.shape or list(a.columns) != list(b.columns):
        return True
    aa = a.astype(str).fillna("")
    bb = b.astype(str).fillna("")
    return not (aa.values == bb.values).all()


# Categorias em 3 níveis (hardcoded) para o dropdown de primary_category_id
# Estrutura: id, nome, children (opcional). Só as folhas são opções do dropdown (path = Nível1 > Nível2 > Nível3).
_CATEGORIAS_RAIZ = [
    {
        "id": "1",
        "nome": "Construção",
        "children": [
            {
                "id": "1.1",
                "nome": "Materiais",
                "children": [
                    {"id": "1.1.1", "nome": "Cimento"},
                    {"id": "1.1.2", "nome": "Areia"},
                    {"id": "1.1.3", "nome": "Brita"},
                    {"id": "1.1.4", "nome": "Argamassa"},
                ],
            },
            {
                "id": "1.2",
                "nome": "Ferramentas",
                "children": [
                    {"id": "1.2.1", "nome": "Manuais"},
                    {"id": "1.2.2", "nome": "Elétricas"},
                    {"id": "1.2.3", "nome": "Medição"},
                ],
            },
            {
                "id": "1.3",
                "nome": "Estrutura",
                "children": [
                    {"id": "1.3.1", "nome": "Ferragens"},
                    {"id": "1.3.2", "nome": "Madeira"},
                    {"id": "1.3.3", "nome": "Metais"},
                ],
            },
        ],
    },
    {
        "id": "2",
        "nome": "Instalações",
        "children": [
            {
                "id": "2.1",
                "nome": "Elétrica",
                "children": [
                    {"id": "2.1.1", "nome": "Fios e Cabos"},
                    {"id": "2.1.2", "nome": "Quadros e Disjuntores"},
                    {"id": "2.1.3", "nome": "Tomadas e Interruptores"},
                ],
            },
            {
                "id": "2.2",
                "nome": "Hidráulica",
                "children": [
                    {"id": "2.2.1", "nome": "Tubos e Conexões"},
                    {"id": "2.2.2", "nome": "Registro e Torneiras"},
                    {"id": "2.2.3", "nome": "Bombas"},
                ],
            },
        ],
    },
    {
        "id": "3",
        "nome": "Acabamento",
        "children": [
            {
                "id": "3.1",
                "nome": "Tintas",
                "children": [
                    {"id": "3.1.1", "nome": "Látex"},
                    {"id": "3.1.2", "nome": "Acrílica"},
                    {"id": "3.1.3", "nome": "Esmalte"},
                ],
            },
            {
                "id": "3.2",
                "nome": "Revestimentos",
                "children": [
                    {"id": "3.2.1", "nome": "Cerâmica"},
                    {"id": "3.2.2", "nome": "Porcelanato"},
                    {"id": "3.2.3", "nome": "Pastilhas"},
                ],
            },
        ],
    },
]


def _flatten_categorias(tree: list, path: list[str] | None = None) -> list[tuple[str, str]]:
    """Achatamento: (id, path_completo) só para folhas. path = 'Nível1 > Nível2 > Nível3'."""
    path = path or []
    out = []
    for node in tree:
        nome = node["nome"]
        id_ = node["id"]
        filhos = node.get("children") or []
        caminho = path + [nome]
        if not filhos:
            out.append((id_, " > ".join(caminho)))
        else:
            out.extend(_flatten_categorias(filhos, caminho))
    return out


def _categoria_maps():
    """Retorna (id_to_path, path_to_id) para conversão banco <-> dropdown."""
    flat = _flatten_categorias(_CATEGORIAS_RAIZ)
    id_to_path = {id_: path for id_, path in flat}
    path_to_id = {path: id_ for id_, path in flat}
    return id_to_path, path_to_id


def _opcoes_categoria() -> list[str]:
    """Opções do dropdown: vazio + path de cada categoria folha."""
    _, path_to_id = _categoria_maps()
    return [""] + list(path_to_id.keys())


def _id_para_rotulo_categoria(primary_category_id) -> str:
    """Converte id da categoria (do banco) para o path exibido no dropdown."""
    if primary_category_id is None or (isinstance(primary_category_id, float) and pd.isna(primary_category_id)):
        return ""
    id_to_path, _ = _categoria_maps()
    return id_to_path.get(str(primary_category_id).strip(), str(primary_category_id))


def _path_para_id_categoria(path) -> str | None:
    """Converte valor do dropdown (path) para id a gravar no banco."""
    if path is None or (isinstance(path, str) and not path.strip()):
        return None
    if isinstance(path, float) and pd.isna(path):
        return None
    _, path_to_id = _categoria_maps()
    return path_to_id.get(str(path).strip(), str(path).strip() or None)

if imp_id is not None:
    # ---------- Vista: detalhe da importação (dados lidos direto do SQLite) ----------
    doc = get_import(imp_id)  # SELECT em importacoes + produtos
    if not doc:
        st.error("Importação não encontrada.")
        if st.button("← Voltar à lista"):
            del st.query_params["id"]
            st.rerun()
        st.stop()

    st.title(f"Importação #{imp_id}")
    st.caption(f"Parser: **{doc.get('parser_label', '')}** · Arquivo: _{doc.get('source_filename', '')}_")
    st.markdown(f"Salvo em / **Atualizado em:** {_formata_atualizado(doc.get('updated_at'))}")

    opts_status = ["Rascunho", "Enviado ao Obraí"]
    idx_status = 1 if doc.get("submitted_to_obrai") else 0
    novo_status = st.selectbox(
        "Status da importação",
        options=opts_status,
        index=idx_status,
        key=f"status_imp_{imp_id}",
    )
    novo_submitted = novo_status == "Enviado ao Obraí"
    if novo_submitted != doc.get("submitted_to_obrai"):
        set_import_submitted(imp_id, novo_submitted)
        st.rerun()

    st.subheader("Produtos")
    st.caption("Dados do SQLite. Alterações são **salvas automaticamente** ao editar.")
    if not doc.get("produtos"):
        st.warning("Nenhum produto nesta importação.")
    else:
        pdf = pd.DataFrame(doc["produtos"])

        # Colunas visíveis: identificação primeiro (categoria, marca, nome), depois SKU/EAN, preços, estoque, imagem
        colunas_visiveis = [
            "primary_category_id",  # Categoria
            "brand_id",             # Marca
            "name",                 # Nome
            "sku",
            "ean",
            "price_type_id",
            "retail_price",
            "wholesale_price",
            "minimum_wholesale_quantity",
            "stock_quantity",
            "main_image",
        ]
        # primary_category_id: exibir como path do dropdown
        if "primary_category_id" in pdf.columns:
            pdf["primary_category_id"] = pdf["primary_category_id"].apply(_id_para_rotulo_categoria)
        # main_image: exibir como boolean (tem imagem ou não)
        if "main_image" in pdf.columns:
            pdf["main_image"] = pdf["main_image"].apply(
                lambda x: bool(x) if pd.notna(x) and str(x).strip() else False
            )

        ordem = [c for c in colunas_visiveis if c in pdf.columns] + [
            c for c in pdf.columns if c not in colunas_visiveis
        ]
        pdf = pdf[ordem]

        opcoes_cat = _opcoes_categoria()
        column_config = {}
        for c in pdf.columns:
            if c == "primary_category_id":
                column_config[c] = st.column_config.SelectboxColumn(
                    "Categoria",
                    options=opcoes_cat,
                    required=False,
                )
            elif c == "brand_id":
                column_config[c] = st.column_config.TextColumn("Marca")
            elif c == "name":
                column_config[c] = st.column_config.TextColumn("Nome")
            elif c == "sku":
                column_config[c] = st.column_config.TextColumn("SKU")
            elif c == "ean":
                column_config[c] = st.column_config.TextColumn("EAN")
            elif c == "price_type_id":
                column_config[c] = st.column_config.TextColumn("Tipo de preço")
            elif c == "retail_price":
                column_config[c] = st.column_config.NumberColumn("Preço varejo", format="%.2f")
            elif c == "wholesale_price":
                column_config[c] = st.column_config.NumberColumn("Preço atacado", format="%.2f")
            elif c == "minimum_wholesale_quantity":
                column_config[c] = st.column_config.NumberColumn("Qtd mín. atacado", format="%d")
            elif c == "stock_quantity":
                column_config[c] = st.column_config.NumberColumn("Estoque", format="%d")
            elif c == "main_image":
                column_config[c] = st.column_config.CheckboxColumn("Imagem principal")
            else:
                column_config[c] = {"hidden": True}

        edited = st.data_editor(
            pdf,
            use_container_width=True,
            hide_index=True,
            height=480,
            key=f"produtos_edit_{imp_id}",
            column_config=column_config,
        )
        if _df_alterado(edited, pdf):
            to_save = edited.copy()
            if "primary_category_id" in to_save.columns:
                to_save["primary_category_id"] = to_save["primary_category_id"].map(_path_para_id_categoria)
            # main_image: boolean do editor -> "1" ou "" para o banco
            if "main_image" in to_save.columns:
                to_save["main_image"] = to_save["main_image"].apply(
                    lambda x: "1" if x is True or (isinstance(x, str) and x and str(x).lower() in ("true", "1", "yes")) else ""
                )
            update_produtos(imp_id, to_save)
            st.rerun()
    st.stop()

# ---------- Vista: lista de importações ----------
st.title("Importações")
st.caption("Registros salvos no SQLite. Clique em **Ver** na linha para abrir o detalhe e a lista de produtos.")

items = list_imports(500)
if not items:
    st.info("Nenhuma importação salva. Use **Importar** e depois **Salvar no banco**.")
else:
    head = st.columns([0.6, 2, 1.2, 1.2, 0.8, 1.5, 0.6, 0.6])
    head[0].markdown("**id**")
    head[1].markdown("**nome**")
    head[2].markdown("**data**")
    head[3].markdown("**parser**")
    head[4].markdown("**produtos**")
    head[5].markdown("**arquivo**")
    head[6].markdown("**obraí**")
    head[7].markdown("**Ver**")
    st.divider()

    for i in items:
        col = st.columns([0.6, 2, 1.2, 1.2, 0.8, 1.5, 0.6, 0.6])
        col[0].write(i["id"])
        col[1].write(i.get("nome") or "—")
        col[2].write((i.get("created_at") or "")[:19].replace("T", " "))
        col[3].write(i.get("parser_label", ""))
        col[4].write(i.get("product_count", 0))
        col[5].write(i.get("source_filename") or "—")
        col[6].write("Sim" if i.get("submitted_to_obrai") else "Não")
        with col[7]:
            if st.button("Ver", key=f"ver_{i['id']}"):
                st.query_params["id"] = str(i["id"])
                st.rerun()
