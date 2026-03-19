#!/usr/bin/env python3
"""
converter_csv

Converte um CSV grande (potencialmente milhoes de linhas) em JSON usando:
 - processamento em chunks (sem carregar tudo na RAM)
 - LLM local via Ollama (recomendado) ou OpenAI-compatible local (ex: LM Studio)

Saida recomendada: JSONL (1 objeto por linha) para nao estourar memoria.

Exemplo:
  python scripts/converter_csv.py "entrada.csv" "saida.jsonl" ^
    --provider ollama --model llama3.2 --chunksize 500
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

try:
    import json_repair  # type: ignore
except Exception:  # pragma: no cover
    json_repair = None


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_TARGET_SCHEMA = r"""
{
  "id": "string ou numero unico",
  "nome_completo": "string",
  "email_normalizado": "string em minusculo",
  "categoria": "string (VIP | COMUM | INATIVO baseado em regras)",
  "valor_total": "numero float com 2 casas",
  "data_formatada": "YYYY-MM-DD",
  "tags": ["array de strings extraidas ou inferidas"]
}
""".strip()


DEFAULT_SYSTEM_PROMPT_TEMPLATE = """
Voce e um transformador de dados preciso.

Receba uma lista de registros (JSON) e transforme CADA UM no formato EXATO abaixo.
Responda APENAS com um array JSON valido de objetos no formato solicitado.
NADA de texto explicativo, NADA de Markdown, NADA de ```json.

Formato desejado para cada item:
{target_schema}

Regras adicionais:
- Limpe campos conforme necessario (ex: emails em minusculo).
- Se um campo nao puder ser inferido, use null ou string vazia.
- Mantenha a ordem original dos itens.
""".strip()


def _read_text_file(path: str | None) -> str | None:
    if not path:
        return None
    p = Path(path)
    return p.read_text(encoding="utf-8").strip()


def _extract_json(text: str) -> Any:
    """
    Tenta extrair JSON do texto.
    Retorna um objeto python (list/dict) ou levanta ValueError.
    """
    text = (text or "").strip()
    if not text:
        raise ValueError("empty model output")

    # Primeiro, tenta JSON direto
    try:
        return json.loads(text)
    except Exception:
        pass

    # Depois, tenta extrair bloco em ```...```
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        try:
            return json.loads(candidate)
        except Exception:
            text = candidate

    # Por ultimo, tenta achar o primeiro [ ... ] ou { ... }
    m = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
    if not m:
        raise ValueError("no json found in output")

    candidate = m.group(1)
    try:
        return json.loads(candidate)
    except Exception:
        if json_repair is not None:
            # json_repair.loads tenta consertar JSON quebrado
            return json_repair.loads(candidate)  # type: ignore[attr-defined]
        raise


def _call_ollama_chat(
    base_url: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    timeout_s: int,
) -> str:
    url = base_url.rstrip("/") + "/api/chat"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "options": {"temperature": temperature},
    }
    resp = requests.post(url, json=payload, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    return (data.get("message") or {}).get("content") or ""


def _call_openai_compatible_chat(
    base_url: str,
    model: str,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_tokens: int,
    timeout_s: int,
) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    choices = data.get("choices") or []
    if not choices:
        return ""
    return (choices[0].get("message") or {}).get("content") or ""


def transform_chunk_with_llm(
    records: list[dict[str, Any]],
    provider: str,
    model: str,
    system_prompt: str,
    temperature: float,
    base_url: str,
    api_key: str,
    max_tokens: int,
    timeout_s: int,
) -> list[dict[str, Any]]:
    records_json = json.dumps(records, ensure_ascii=False)
    user_prompt = (
        "Transforme esta lista de registros (em JSON) para o formato solicitado.\n"
        "Responda apenas com um array JSON valido.\n\n"
        f"REGISTROS:\n{records_json}\n"
    )

    if provider == "ollama":
        out_text = _call_ollama_chat(
            base_url=base_url,
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            timeout_s=timeout_s,
        )
    elif provider in ("openai", "openai_compatible", "lmstudio", "lm_studio"):
        out_text = _call_openai_compatible_chat(
            base_url=base_url,
            model=model,
            api_key=api_key,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_s=timeout_s,
        )
    else:
        raise ValueError(f"Unknown provider: {provider}")

    parsed = _extract_json(out_text)
    if isinstance(parsed, list):
        return parsed  # type: ignore[return-value]
    if isinstance(parsed, dict):
        # Tolerancia: alguns modelos retornam {"results": [...]}
        for k in ("results", "items", "data"):
            v = parsed.get(k)
            if isinstance(v, list):
                return v  # type: ignore[return-value]
    raise ValueError("Model output is not a list of objects")


def iter_csv_chunks(
    input_csv: str,
    chunksize: int,
    sep: str,
    encoding: str,
) -> Iterable[pd.DataFrame]:
    # dtype=str evita conversoes estranhas e reduz surpresas no LLM
    yield from pd.read_csv(
        input_csv,
        chunksize=chunksize,
        sep=sep,
        encoding=encoding,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Converter CSV grande em JSON usando LLM local.")
    parser.add_argument("input_csv", type=str, help="Caminho do CSV de entrada")
    parser.add_argument("output_jsonl", type=str, help="Caminho do JSONL de saida (1 objeto por linha)")

    parser.add_argument("--sep", type=str, default=",", help="Separador do CSV (ex: ';' para Brasil)")
    parser.add_argument("--encoding", type=str, default="utf-8", help="Encoding do CSV (ex: utf-8, latin1)")
    parser.add_argument("--chunksize", type=int, default=500, help="Registros por chunk (quantidade enviada ao LLM)")

    parser.add_argument("--provider", type=str, default="ollama", help="ollama ou openai")
    parser.add_argument("--model", type=str, default="llama3.2", help="Nome do modelo no servidor local")
    parser.add_argument("--base-url", type=str, default="http://localhost:11434", help="Base URL do servidor local")
    parser.add_argument("--api-key", type=str, default="lm-studio", help="Api key dummy para openai compatible")
    parser.add_argument("--temperature", type=float, default=0.1, help="Temperatura do LLM")
    parser.add_argument("--max-tokens", type=int, default=4096, help="Max tokens (apenas para openai compatible)")
    parser.add_argument("--timeout-s", type=int, default=180, help="Timeout da requisicao ao LLM")

    parser.add_argument("--target-schema", type=str, default="", help="Override da descricao do schema (texto).")
    parser.add_argument("--target-schema-file", type=str, default="", help="Arquivo com a descricao do schema.")
    parser.add_argument("--system-prompt-file", type=str, default="", help="Arquivo com o system prompt completo.")
    parser.add_argument("--dry-run", action="store_true", help="Nao grava; apenas imprime o primeiro objeto de cada chunk.")

    args = parser.parse_args()

    input_csv = args.input_csv
    output_jsonl = args.output_jsonl
    chunksize = args.chunksize

    if not Path(input_csv).exists():
        raise SystemExit(f"Input CSV nao encontrado: {input_csv}")

    target_schema = args.target_schema.strip() or _read_text_file(args.target_schema_file) or DEFAULT_TARGET_SCHEMA
    system_prompt = _read_text_file(args.system_prompt_file) or DEFAULT_SYSTEM_PROMPT_TEMPLATE.format(
        target_schema=target_schema
    )

    # Se nao existir, cria arquivo; se existir, anexa (para retomar manualmente).
    Path(output_jsonl).parent.mkdir(parents=True, exist_ok=True)
    if not args.dry_run:
        open(output_jsonl, "a", encoding="utf-8").close()

    chunk_num = 0
    total_out = 0
    for chunk_df in iter_csv_chunks(
        input_csv=input_csv,
        chunksize=chunksize,
        sep=args.sep,
        encoding=args.encoding,
    ):
        chunk_num += 1
        # Converte para lista de dicts (mantem colunas do CSV)
        records = json.loads(chunk_df.to_json(orient="records", force_ascii=False))

        if not records:
            continue

        print(f"[chunk {chunk_num}] input_records={len(records)} ...", flush=True)
        try:
            transformed = transform_chunk_with_llm(
                records=records,
                provider=args.provider,
                model=args.model,
                system_prompt=system_prompt,
                temperature=args.temperature,
                base_url=args.base_url,
                api_key=args.api_key,
                max_tokens=args.max_tokens,
                timeout_s=args.timeout_s,
            )
        except Exception as e:
            print(f"[chunk {chunk_num}] ERRO: {e}. Pulando chunk.", file=sys.stderr, flush=True)
            continue

        if not isinstance(transformed, list):
            print(f"[chunk {chunk_num}] ERRO: transformed nao e list. Pulando.", file=sys.stderr, flush=True)
            continue

        if args.dry_run:
            if transformed:
                print(f"[chunk {chunk_num}] first_output={transformed[0]}")
            continue

        # Escreve no formato JSONL (1 linha por objeto) para escalar bem
        with open(output_jsonl, "a", encoding="utf-8") as f:
            for obj in transformed:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                total_out += 1

        print(f"[chunk {chunk_num}] done. written={len(transformed)} total_out={total_out}", flush=True)

    print(f"Pronto. Total objetos escritos: {total_out}")


if __name__ == "__main__":
    main()

