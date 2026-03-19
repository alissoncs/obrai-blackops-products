#!/usr/bin/env python3
"""
Enriquece produtos de uma importação usando IA local via OpenAI-compatible (LM Studio).

Uso:
  python scripts/enriquecer_importacao.py <ID_IMPORTACAO>
  python scripts/enriquecer_importacao.py --id 5

Requer o LM Studio com o servidor OpenAI-compatible rodando em:
  http://localhost:1234/v1
(ajustável via flags --base-url e --model)

Preenche campos em branco: description, slug, tags, primary_category_id.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Permitir importar db a partir da raiz do projeto
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests

from db import get_import, update_produtos

# Modelo / servidor (LM Studio)
MODEL = "Qwen2.5-7B-Instruct"
BASE_URL = "http://localhost:1234/v1"
API_KEY = "lm-studio"  # dummy (LM Studio geralmente nao exige auth)

# Categorias folha (id, path) para o LLM escolher primary_category_id
CATEGORIAS_FLAT = [
    ("1.1.1", "Construção > Materiais > Cimento"),
    ("1.1.2", "Construção > Materiais > Areia"),
    ("1.1.3", "Construção > Materiais > Brita"),
    ("1.1.4", "Construção > Materiais > Argamassa"),
    ("1.2.1", "Construção > Ferramentas > Manuais"),
    ("1.2.2", "Construção > Ferramentas > Elétricas"),
    ("1.2.3", "Construção > Ferramentas > Medição"),
    ("1.3.1", "Construção > Estrutura > Ferragens"),
    ("1.3.2", "Construção > Estrutura > Madeira"),
    ("1.3.3", "Construção > Estrutura > Metais"),
    ("2.1.1", "Instalações > Elétrica > Fios e Cabos"),
    ("2.1.2", "Instalações > Elétrica > Quadros e Disjuntores"),
    ("2.1.3", "Instalações > Elétrica > Tomadas e Interruptores"),
    ("2.2.1", "Instalações > Hidráulica > Tubos e Conexões"),
    ("2.2.2", "Instalações > Hidráulica > Registro e Torneiras"),
    ("2.2.3", "Instalações > Hidráulica > Bombas"),
    ("3.1.1", "Acabamento > Tintas > Látex"),
    ("3.1.2", "Acabamento > Tintas > Acrílica"),
    ("3.1.3", "Acabamento > Tintas > Esmalte"),
    ("3.2.1", "Acabamento > Revestimentos > Cerâmica"),
    ("3.2.2", "Acabamento > Revestimentos > Porcelanato"),
    ("3.2.3", "Acabamento > Revestimentos > Pastilhas"),
]


def _slug(s: str, max_len: int = 80) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s).strip().lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[-\s]+", "-", s).strip("-")[:max_len]
    return s or "item"


def _call_openai_compatible_chat(
    prompt: str,
    *,
    model: str,
    base_url: str,
    api_key: str,
    temperature: float,
    max_tokens: int,
    timeout_s: int,
) -> str:
    """Chama endpoint OpenAI-compatible (/v1/chat/completions) e retorna content."""
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "Responda APENAS com JSON valido, sem texto extra, sem markdown.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return ""
        return (choices[0].get("message") or {}).get("content") or ""
    except requests.exceptions.RequestException as e:
        raise SystemExit(
            f"Erro ao chamar LM Studio (está rodando em {base_url}?): {e}"
        ) from e


def _extrair_json(texto: str) -> dict | None:
    """Tenta extrair um objeto JSON do texto (pode estar dentro de ```json ... ```)."""
    texto = texto.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", texto)
    if m:
        texto = m.group(1).strip()
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        pass
    # Última tentativa: primeiro { até último }
    m = re.search(r"\{[\s\S]*\}", texto)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return None


def _enriquecer_produto(
    prod: dict,
    categorias_texto: str,
    *,
    model: str,
    base_url: str,
    api_key: str,
    temperature: float,
    max_tokens: int,
    timeout_s: int,
) -> dict:
    """Gera description, slug, tags e primary_category_id via LM Studio e atualiza o prod."""
    name = (prod.get("name") or "").strip()
    if not name:
        return prod

    # Só enriquecer campos vazios
    need_desc = not (prod.get("description") or "").strip()
    need_slug = not (prod.get("slug") or "").strip()
    need_tags = not (prod.get("tags") or "").strip()
    need_cat = not (prod.get("primary_category_id") or "").strip()

    if not (need_desc or need_slug or need_tags or need_cat):
        return prod

    prompt = f"""Com base no produto abaixo, retorne um ÚNICO objeto JSON com as chaves que eu pedir.
Produto: nome="{name}", sku="{prod.get('sku') or ''}"

Regras:
- description: texto curto (1 ou 2 frases) para e-commerce, em português.
- slug: único para URL, minúsculo, hífens, sem acentos (ex: cimento-cp-40kg).
- tags: palavras-chave separadas por vírgula, em português (ex: cimento, construção, obra).
- primary_category_id: DEVE ser exatamente um dos IDs da lista abaixo, o que melhor classifica o produto.

Lista de categorias (use só o id):
{categorias_texto}

Retorne somente o JSON, sem explicação. Exemplo: {{"description": "...", "slug": "...", "tags": "...", "primary_category_id": "1.1.1"}}
"""
    resp = _call_openai_compatible_chat(
        prompt,
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout_s=timeout_s,
    )
    data = _extrair_json(resp)
    if not data:
        return prod

    if need_desc and isinstance(data.get("description"), str):
        prod["description"] = data["description"].strip()[:2000]
    if need_slug and isinstance(data.get("slug"), str):
        prod["slug"] = _slug(data["slug"])[:80] or _slug(name)[:80]
    if need_tags and isinstance(data.get("tags"), str):
        prod["tags"] = data["tags"].strip()[:500]
    if need_cat and isinstance(data.get("primary_category_id"), str):
        id_cat = data["primary_category_id"].strip()
        ids_validos = {c[0] for c in CATEGORIAS_FLAT}
        if id_cat in ids_validos:
            prod["primary_category_id"] = id_cat

    return prod


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enriquece produtos de uma importação com IA local (LM Studio)."
    )
    parser.add_argument("id", nargs="?", type=int, help="ID da importação")
    parser.add_argument("--id", dest="id_flag", type=int, help="ID da importação")
    parser.add_argument("--model", default=MODEL, help=f"Nome do modelo no LM Studio (default: {MODEL})")
    parser.add_argument("--base-url", default=BASE_URL, help=f"Base URL OpenAI-compatible (default: {BASE_URL})")
    parser.add_argument("--api-key", default=API_KEY, help="Api key dummy para LM Studio (geralmente nao exige)")
    parser.add_argument("--temperature", type=float, default=0.1, help="Temperatura do LLM")
    parser.add_argument("--max-tokens", type=int, default=1024, help="Max tokens da resposta")
    parser.add_argument("--timeout-s", type=int, default=180, help="Timeout da requisicao ao LLM")
    parser.add_argument("--dry-run", action="store_true", help="Só mostrar o que seria feito, não gravar")
    args = parser.parse_args()

    imp_id = args.id or args.id_flag
    if imp_id is None:
        parser.error("Informe o ID da importação (posicional ou --id).")

    doc = get_import(imp_id)
    if not doc:
        raise SystemExit(f"Importação #{imp_id} não encontrada.")

    produtos = doc.get("produtos") or []
    if not produtos:
        raise SystemExit(f"Importação #{imp_id} não tem produtos.")

    categorias_texto = "\n".join(f"  {c[0]} = {c[1]}" for c in CATEGORIAS_FLAT)

    print(f"Importação #{imp_id}: {len(produtos)} produto(s). Enriquecendo com {args.model}...")
    for i, p in enumerate(produtos):
        nome = (p.get("name") or "")[:50]
        print(f"  [{i+1}/{len(produtos)}] {nome}...")
        _enriquecer_produto(
            p,
            categorias_texto,
            model=args.model,
            base_url=args.base_url,
            api_key=args.api_key,
            temperature=args.temperature,
            max_tokens=args.max_tokens,
            timeout_s=args.timeout_s,
        )

    if args.dry_run:
        print("\n[DRY-RUN] Nenhuma alteração gravada.")
        for p in produtos[:3]:
            print(f"  name={p.get('name')!r} description={p.get('description')!r} slug={p.get('slug')!r} primary_category_id={p.get('primary_category_id')!r}")
        return

    import pandas as pd
    df = pd.DataFrame(produtos)
    update_produtos(imp_id, df)
    print(f"\nImportação #{imp_id} atualizada no banco.")


if __name__ == "__main__":
    main()
