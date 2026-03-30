#!/usr/bin/env python3
"""
Classifica produtos Tigre contra a taxonomia real (input/categories.json) via API OpenAI-compatible (Groq por defeito).

Grava output/tigre_categories.json; relatórios em output/aux/ com timestamp no nome; categories_for_llm.json fixo.
Se tigre_categories.json já existir, por omissão continua: não reclassifica slugs já presentes (use --reclassify-all para ignorar o ficheiro anterior).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]

import httpx
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_AUX = SCRIPT_DIR / "output" / "aux"


def utc_report_filename(stem: str, *, ts: str | None = None) -> str:
    """Nome de ficheiro de relatório em aux/: stem_YYYYMMDDTHHMMSSZ.json (UTC)."""
    if ts is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stem}_{ts}.json"


DEFAULT_BASE_URL = "https://api.groq.com/openai/v1"
DEFAULT_MODEL = "llama-3.3-70b-versatile"
DEFAULT_BATCH_SIZE = 24
DEFAULT_CONCURRENCY = 4
DEFAULT_TIMEOUT = 120.0
DEFAULT_MAX_TOKENS = 8192
DEFAULT_MAX_RETRIES = 6
DEFAULT_REVIEW_IF_BELOW = 3
DEFAULT_TAXONOMY_PROMPT_MODE = "slugs"
DESCRIPTION_PROMPT_MAX_LEN = 400
CONFIDENCE_CAP_MISSING_MEDIA = 2


@dataclass
class CategoryLeaf:
    slug: str
    name: str
    path: str


def _load_dotenv_files() -> None:
    if not load_dotenv:
        return
    load_dotenv(SCRIPT_DIR / ".env")
    load_dotenv(Path.cwd() / ".env")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def resolve_api_key(cli: str | None) -> str:
    if cli and cli.strip():
        return cli.strip()
    for key in ("GROQ_API_KEY", "TIGRE_LLM_API_KEY", "OPENAI_API_KEY"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    return ""


def resolve_taxonomy_prompt_mode(cli: str | None) -> str:
    """CLI > env TIGRE_LLM_TAXONOMY_PROMPT; valores: slugs | full."""
    if cli is not None and str(cli).strip():
        m = str(cli).strip().lower()
        if m in ("slugs", "full"):
            return m
    env = os.environ.get("TIGRE_LLM_TAXONOMY_PROMPT", "").strip().lower()
    if env in ("slugs", "full"):
        return env
    return DEFAULT_TAXONOMY_PROMPT_MODE


def flatten_categories(
    nodes: list[dict[str, Any]],
    ancestors: list[str],
    *,
    allow_non_leaf: bool,
) -> list[CategoryLeaf]:
    out: list[CategoryLeaf] = []
    for node in nodes:
        if node.get("status") != "active":
            continue
        if node.get("disabledAt") is not None:
            continue
        name = (node.get("name") or "").strip()
        slug = (node.get("slug") or "").strip()
        children = node.get("children") or []
        path_names = ancestors + [name] if name else ancestors
        path_str = " > ".join(path_names)
        is_leaf = len(children) == 0
        if is_leaf and slug:
            out.append(CategoryLeaf(slug=slug, name=name, path=path_str))
        elif allow_non_leaf and slug:
            out.append(CategoryLeaf(slug=slug, name=name, path=path_str))
        if children:
            out.extend(
                flatten_categories(children, path_names, allow_non_leaf=allow_non_leaf)
            )
    return out


def collapse_duplicate_slug_leaves(
    leaves: list[CategoryLeaf],
) -> tuple[list[CategoryLeaf], list[str]]:
    """Funde folhas com o mesmo slug (dados de origem podem repetir slug em ramos distintos)."""
    by_slug: dict[str, CategoryLeaf] = {}
    warnings: list[str] = []
    for leaf in leaves:
        if leaf.slug not in by_slug:
            by_slug[leaf.slug] = leaf
        else:
            prev = by_slug[leaf.slug]
            warnings.append(f"slug duplicado fundido: {leaf.slug!r}")
            by_slug[leaf.slug] = CategoryLeaf(
                slug=leaf.slug,
                name=prev.name,
                path=prev.path + " || " + leaf.path,
            )
    collapsed = sorted(by_slug.values(), key=lambda x: x.slug)
    return collapsed, warnings


def taxonomy_prompt_block(leaves: list[CategoryLeaf]) -> str:
    lines = [f"{c.slug} — {c.path}" for c in leaves]
    return "\n".join(lines)


def taxonomy_slugs_block(leaves: list[CategoryLeaf]) -> str:
    """Uma linha por slug (ordenado); muito mais curto que slug — path completo."""
    return "\n".join(c.slug for c in leaves)


def _system_rules_and_rubric() -> str:
    return (
        "Rubrica confidence (inteiro 1 a 5):\n"
        "5 = encaixa perfeitamente na categoria escolhida\n"
        "4 = muito provável\n"
        "3 = razoável mas há alternativas plausíveis\n"
        "2 = fraco ou ambíguo\n"
        "1 = pouca informação ou chute\n\n"
        "Regras:\n"
        "- categorySlug tem de ser EXACTAMENTE um dos valores listados abaixo (copiar literalmente).\n"
        "- Incluir um objeto por produto da lista do utilizador; product_slug igual ao slug de entrada.\n"
        "- Responde só com JSON válido (objeto com chave classifications).\n\n"
    )


def system_message_slugs(slugs_block: str) -> str:
    return (
        "És um classificador de produtos de loja de materiais de construção (Tigre). "
        "Cada produto recebe exatamente UMA categoria. A lista abaixo são **slugs** (IDs curtos); "
        "usa o nome, a descrição (quando existir na linha do produto) e o teu conhecimento para escolher o slug mais adequado.\n\n"
        + _system_rules_and_rubric()
        + "Slugs permitidos (um por linha):\n"
        + slugs_block
    )


def system_message_full(taxonomy_block: str) -> str:
    return (
        "És um classificador de produtos de loja de materiais de construção (Tigre). "
        "Cada produto deve receber exatamente UMA categoria da lista abaixo (slug terminal).\n\n"
        + _system_rules_and_rubric()
        + "Categorias (slug — caminho):\n"
        + taxonomy_block
    )


def _sanitize_line(s: str, max_len: int = 500) -> str:
    s = s.replace("\n", " ").replace("\r", " ").strip()
    return s[:max_len]



def product_has_description(p: dict[str, Any]) -> bool:
    d = p.get("description")
    return isinstance(d, str) and bool(d.strip())


def product_has_image(p: dict[str, Any]) -> bool:
    mi = p.get("mainImage")
    if isinstance(mi, str) and mi.strip():
        return True
    imgs = p.get("images")
    return isinstance(imgs, list) and len(imgs) > 0


def product_lacks_description_and_image(p: dict[str, Any]) -> bool:
    return not product_has_description(p) and not product_has_image(p)


def description_for_prompt(p: dict[str, Any]) -> str:
    raw = p.get("description")
    if not isinstance(raw, str):
        raw = str(raw or "")
    t = _sanitize_line(raw, max_len=DESCRIPTION_PROMPT_MAX_LEN)
    return t.replace("|", " ")


def build_user_message(products: list[dict[str, Any]]) -> str:
    lines = []
    for p in products:
        slug = _sanitize_line(str(p.get("slug", "")))
        sku = _sanitize_line(str(p.get("sku", "")))
        name = _sanitize_line(str(p.get("name", "")))
        desc = description_for_prompt(p)
        lines.append(f"{slug}|{sku}|{name}|{desc}")
    return (
        "Produtos (uma linha cada, formato slug|sku|nome|descrição). "
        "O último campo é a descrição do produto (pode estar vazio; já vem truncada). "
        "Usa nome e descrição para escolher a categoria.\n"
        'Devolve JSON com chave "classifications": array de objetos com '
        "product_slug (string, obrigatório), categorySlug (string, slug EXATO da lista do system), "
        "confidence (inteiro 1-5), reason (string opcional, curta).\n\n"
        + "\n".join(lines)
    )


def _extract_json_object(text: str) -> dict[str, Any] | None:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if m:
        text = m.group(1).strip()
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            data = json.loads(text[start : end + 1])
            return data if isinstance(data, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def normalize_confidence(raw: Any) -> tuple[int, bool]:
    assumed = False
    try:
        n = int(raw)
    except (TypeError, ValueError):
        n = 3
        assumed = True
    if n < 1 or n > 5:
        n = 3
        assumed = True
    return n, assumed


@dataclass
class BatchResult:
    batch_id: int
    expected_count: int = 0
    usage_prompt: int | None = None
    usage_completion: int | None = None
    usage_total: int | None = None
    ok: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)


async def call_chat_completion(
    client: httpx.AsyncClient,
    *,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: float,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    r = await client.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    usage = data.get("usage")
    if usage and isinstance(usage, dict):
        u = {
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "total_tokens": usage.get("total_tokens"),
        }
    else:
        u = None
    return data, u

async def classify_batch(
    *,
    batch_id: int,
    products: list[dict[str, Any]],
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    chat_url: str,
    headers: dict[str, str],
    model: str,
    system_content: str,
    allowed_slugs: set[str],
    slug_to_path: dict[str, str],
    max_tokens: int,
    max_retries: int,
    timeout: float,
    dry_run: bool,
) -> BatchResult:
    result = BatchResult(batch_id=batch_id, expected_count=len(products))
    user_content = build_user_message(products)
    expected_slugs = {str(p.get("slug", "")).strip() for p in products}

    if dry_run:
        result.errors.append(
            {"batch_id": batch_id, "message": "dry_run", "produtos": len(products)}
        )
        return result

    base_payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }

    last_err: str | None = None
    async with sem:
        for attempt in range(max_retries):
            payloads: list[dict[str, Any]] = [
                {**base_payload, "response_format": {"type": "json_object"}},
                dict(base_payload),
            ]
            data: dict[str, Any] | None = None
            usage: dict[str, Any] | None = None
            http_ok = False
            for pi, payload in enumerate(payloads):
                try:
                    data, usage = await call_chat_completion(
                        client,
                        url=chat_url,
                        headers=headers,
                        payload=payload,
                        timeout=timeout,
                    )
                    http_ok = True
                    break
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400 and pi == 0:
                        last_err = f"HTTP 400 (retry sem json_object): {e.response.text[:120]}"
                        continue
                    last_err = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                    if e.response.status_code == 429 and attempt < max_retries - 1:
                        await asyncio.sleep((2**attempt) + random.uniform(0, 0.5))
                    elif 500 <= e.response.status_code < 600 and attempt < max_retries - 1:
                        await asyncio.sleep((2**attempt) + random.uniform(0, 0.5))
                    break
                except httpx.RequestError as e:
                    last_err = str(e)
                    if attempt < max_retries - 1:
                        await asyncio.sleep((2**attempt) + random.uniform(0, 0.5))
                    break

            if not http_ok or data is None:
                continue

            if usage:
                pt = usage.get("prompt_tokens")
                ct = usage.get("completion_tokens")
                tt = usage.get("total_tokens")
                result.usage_prompt = int(pt) if pt is not None else None
                result.usage_completion = int(ct) if ct is not None else None
                result.usage_total = int(tt) if tt is not None else None

            try:
                choices = data.get("choices") or []
                if not choices:
                    last_err = "resposta sem choices"
                    raise ValueError(last_err)
                content = (choices[0].get("message") or {}).get("content") or ""
                parsed = _extract_json_object(content)
                if not parsed:
                    last_err = "JSON inválido na resposta"
                    raise ValueError(last_err)

                arr = parsed.get("classifications")
                if not isinstance(arr, list):
                    last_err = "classifications não é array"
                    raise ValueError(last_err)

                seen: set[str] = set()
                for item in arr:
                    if not isinstance(item, dict):
                        continue
                    ps = str(item.get("product_slug", "")).strip()
                    if not ps or ps not in expected_slugs:
                        continue
                    cat = str(item.get("categorySlug", "")).strip()
                    conf_llm, conf_assumed = normalize_confidence(item.get("confidence"))
                    reason = item.get("reason")
                    if cat not in allowed_slugs:
                        result.errors.append(
                            {
                                "product_slug": ps,
                                "batch_id": batch_id,
                                "message": f"categorySlug inválido: {cat!r}",
                            }
                        )
                        seen.add(ps)
                        continue
                    prod_row = next(
                        (p for p in products if str(p.get("slug", "")).strip() == ps),
                        {},
                    )
                    conf = conf_llm
                    if product_lacks_description_and_image(prod_row):
                        conf = min(conf_llm, CONFIDENCE_CAP_MISSING_MEDIA)
                    entry: dict[str, Any] = {
                        "slug": ps,
                        "sku": str(prod_row.get("sku", "")),
                        "categorySlug": cat,
                        "categoryPath": slug_to_path.get(cat, ""),
                        "confidence": conf,
                        "batch_id": batch_id,
                    }
                    if conf_assumed:
                        entry["confidence_assumed"] = True
                    if conf < conf_llm:
                        entry["confidence_capped_missing_media"] = True
                    if reason is not None and str(reason).strip():
                        entry["reason"] = str(reason).strip()[:300]
                    result.ok.append(entry)
                    seen.add(ps)

                for ps in expected_slugs - seen:
                    result.errors.append(
                        {
                            "product_slug": ps,
                            "batch_id": batch_id,
                            "message": "faltou classificação na resposta",
                        }
                    )
                return result
            except ValueError as e:
                last_err = str(e)
                if attempt < max_retries - 1:
                    await asyncio.sleep((2**attempt) + random.uniform(0, 0.5))
                continue

    for p in products:
        ps = str(p.get("slug", "")).strip()
        result.errors.append(
            {
                "product_slug": ps,
                "batch_id": batch_id,
                "message": last_err or "falha após retries",
            }
        )
    return result


def host_only(base_url: str) -> str:
    try:
        p = urlparse(base_url if "://" in base_url else f"https://{base_url}")
        return p.netloc or p.path or base_url
    except Exception:
        return "unknown"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enriquecer categorias dos produtos Tigre via LLM.")
    p.add_argument(
        "--categories-tree",
        type=Path,
        default=SCRIPT_DIR / "input" / "categories.json",
        help="JSON da árvore de categorias",
    )
    p.add_argument(
        "--products-in",
        type=Path,
        default=SCRIPT_DIR / "output" / "tigre_products.json",
        help="JSON de produtos do scraper",
    )
    p.add_argument(
        "--categories-out",
        type=Path,
        default=SCRIPT_DIR / "output" / "tigre_categories.json",
        help="Saída: classificações por slug de produto",
    )
    p.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Relatório de execução (default: output/aux/relatorio_categorias_<UTC>.json)",
    )
    p.add_argument(
        "--categories-llm-export",
        type=Path,
        default=DEFAULT_OUTPUT_AUX / "categories_for_llm.json",
        help="Export da taxonomia simplificada",
    )
    p.add_argument("--no-export", action="store_true", help="Não gravar categories_for_llm.json")
    p.add_argument(
        "--review-queue-out",
        type=Path,
        default=None,
        help="Fila de revisão manual (default: output/aux/categorias_revisao_manual_<UTC>.json)",
    )
    p.add_argument(
        "--no-review-queue-export",
        action="store_true",
        help="Não gravar ficheiro da fila de revisão (nome por omissão com timestamp)",
    )
    p.add_argument("--limit", type=int, default=None, help="Máximo de produtos a classificar")
    p.add_argument(
        "--review-if-below",
        type=int,
        default=DEFAULT_REVIEW_IF_BELOW,
        help="confidence < N gera [REVISAR] e entra na fila de revisão (default: 3)",
    )
    p.add_argument("--quiet", action="store_true", help="Menos linhas por produto no stdout")
    p.add_argument(
        "--reclassify-all",
        action="store_true",
        help="Ignorar categories-out existente: reclassificar todos os produtos da fila e substituir o ficheiro só com esta execução",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Só carregar dados e lotes; não chama a API",
    )
    p.add_argument(
        "--allow-non-leaf",
        action="store_true",
        help="Permitir slugs de categorias com filhos (além das folhas)",
    )
    p.add_argument("--api-key", default=None, help="Sobrescreve GROQ_API_KEY / env")
    p.add_argument("--base-url", default=None, help="OpenAI-compatible base URL")
    p.add_argument("--model", default=None, help="ID do modelo")
    p.add_argument("--batch-size", type=int, default=None)
    p.add_argument("--concurrency", type=int, default=None)
    p.add_argument("--timeout", type=float, default=None)
    p.add_argument("--max-tokens", type=int, default=None)
    p.add_argument("--max-retries", type=int, default=None)
    p.add_argument(
        "--taxonomy-prompt",
        choices=("slugs", "full"),
        default=None,
        help="slugs=lista compacta (default, menos tokens); full=slug — path (mais contexto)",
    )
    return p.parse_args()


async def _run_batches(
    batches: list[list[dict[str, Any]]],
    *,
    concurrency: int,
    chat_url: str,
    headers: dict[str, str],
    model: str,
    system_content: str,
    allowed_slugs: set[str],
    slug_to_path: dict[str, str],
    max_tokens: int,
    max_retries: int,
    timeout: float,
    dry_run: bool,
) -> list[BatchResult]:
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:

        async def one(bid: int, batch: list[dict[str, Any]]) -> BatchResult:
            return await classify_batch(
                batch_id=bid,
                products=batch,
                client=client,
                sem=sem,
                chat_url=chat_url,
                headers=headers,
                model=model,
                system_content=system_content,
                allowed_slugs=allowed_slugs,
                slug_to_path=slug_to_path,
                max_tokens=max_tokens,
                max_retries=max_retries,
                timeout=timeout,
                dry_run=dry_run,
            )

        coros = [one(i, b) for i, b in enumerate(batches)]
        tasks = [asyncio.create_task(c) for c in coros]
        results: list[BatchResult] = []
        with tqdm(total=len(tasks), desc="Lotes", unit="lote") as bar:
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)
                bar.update(1)
        return results


def main() -> None:
    _load_dotenv_files()
    args = parse_args()
    effective_resume = (not args.reclassify_all) and args.categories_out.is_file()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.report_out is None:
        args.report_out = DEFAULT_OUTPUT_AUX / utc_report_filename("relatorio_categorias", ts=run_ts)
    if args.review_queue_out is None:
        args.review_queue_out = DEFAULT_OUTPUT_AUX / utc_report_filename("categorias_revisao_manual", ts=run_ts)

    api_key = resolve_api_key(args.api_key)
    base_url = (args.base_url or os.environ.get("TIGRE_LLM_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
    model = args.model or os.environ.get("TIGRE_LLM_MODEL") or DEFAULT_MODEL
    batch_size = args.batch_size if args.batch_size is not None else _env_int("TIGRE_LLM_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    concurrency = args.concurrency if args.concurrency is not None else _env_int("TIGRE_LLM_CONCURRENCY", DEFAULT_CONCURRENCY)
    timeout = args.timeout if args.timeout is not None else _env_float("TIGRE_LLM_TIMEOUT", float(DEFAULT_TIMEOUT))
    max_tokens = args.max_tokens if args.max_tokens is not None else _env_int("TIGRE_LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS)
    max_retries = args.max_retries if args.max_retries is not None else _env_int("TIGRE_LLM_MAX_RETRIES", DEFAULT_MAX_RETRIES)

    if batch_size < 1:
        raise SystemExit("--batch-size deve ser >= 1")
    if concurrency < 1:
        raise SystemExit("--concurrency deve ser >= 1")

    if not args.dry_run and not api_key:
        raise SystemExit(
            "Defina GROQ_API_KEY (ou TIGRE_LLM_API_KEY / OPENAI_API_KEY) ou passe --api-key."
        )

    t0 = time.perf_counter()
    tree_path = args.categories_tree
    if not tree_path.is_file():
        raise SystemExit(f"Ficheiro não encontrado: {tree_path}")

    with tree_path.open(encoding="utf-8") as f:
        tree = json.load(f)
    if not isinstance(tree, list):
        raise SystemExit("categories.json deve ser um array na raiz")

    leaves_raw = flatten_categories(tree, [], allow_non_leaf=args.allow_non_leaf)
    leaves, slug_warnings = collapse_duplicate_slug_leaves(leaves_raw)
    slug_to_path = {c.slug: c.path for c in leaves}
    if slug_warnings and not args.quiet:
        for w in slug_warnings[:20]:
            print(f"[AVISO] {w}", flush=True)
        if len(slug_warnings) > 20:
            print(f"[AVISO] ... e mais {len(slug_warnings) - 20} slugs fundidos", flush=True)

    if not args.no_export:
        args.categories_llm_export.parent.mkdir(parents=True, exist_ok=True)
        with args.categories_llm_export.open("w", encoding="utf-8") as f:
            json.dump(
                [{"slug": x.slug, "name": x.name, "path": x.path} for x in leaves],
                f,
                ensure_ascii=False,
                indent=2,
            )

    products_path = args.products_in
    if not products_path.is_file():
        raise SystemExit(f"Ficheiro não encontrado: {products_path}")

    with products_path.open(encoding="utf-8") as f:
        pdata = json.load(f)
    products = pdata.get("products") if isinstance(pdata, dict) else None
    if not isinstance(products, list):
        raise SystemExit("tigre_products.json deve ter chave 'products' (array)")

    active_products = [p for p in products if isinstance(p, dict) and p.get("status") == "active"]
    produtos_carregados = len(active_products)

    already: set[str] = set()
    if effective_resume:
        try:
            with args.categories_out.open(encoding="utf-8") as f:
                prev = json.load(f)
            for it in prev.get("items") or []:
                if isinstance(it, dict) and it.get("slug"):
                    already.add(str(it["slug"]).strip())
        except (json.JSONDecodeError, OSError):
            pass

    to_process = [p for p in active_products if str(p.get("slug", "")).strip() not in already]
    if args.limit is not None:
        to_process = to_process[: max(0, args.limit)]

    produtos_enfileirados = len(to_process)
    allowed_slugs = set(slug_to_path.keys())
    tax_mode = resolve_taxonomy_prompt_mode(args.taxonomy_prompt)
    if tax_mode == "full":
        system_content = system_message_full(taxonomy_prompt_block(leaves))
    else:
        system_content = system_message_slugs(taxonomy_slugs_block(leaves))

    batches: list[list[dict[str, Any]]] = []
    for i in range(0, len(to_process), batch_size):
        batches.append(to_process[i : i + batch_size])

    chat_url = f"{base_url}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    batch_results = asyncio.run(
        _run_batches(
            batches,
            concurrency=concurrency,
            chat_url=chat_url,
            headers=headers,
            model=model,
            system_content=system_content,
            allowed_slugs=allowed_slugs,
            slug_to_path=slug_to_path,
            max_tokens=max_tokens,
            max_retries=max_retries,
            timeout=timeout,
            dry_run=args.dry_run,
        )
    )

    items_by_slug: dict[str, dict[str, Any]] = {}
    if effective_resume:
        try:
            with args.categories_out.open(encoding="utf-8") as f:
                prev = json.load(f)
            for it in prev.get("items") or []:
                if isinstance(it, dict) and it.get("slug"):
                    items_by_slug[str(it["slug"]).strip()] = it
        except (json.JSONDecodeError, OSError):
            pass

    all_errors: list[dict[str, Any]] = []
    distribuicao = {str(n): 0 for n in range(1, 6)}
    prompt_total = completion_total = total_tokens_acc = 0
    usage_known = True
    por_lote: list[dict[str, Any]] = []

    batch_results.sort(key=lambda br: br.batch_id)
    for br in batch_results:
        for e in br.errors:
            if e.get("message") != "dry_run":
                all_errors.append(e)
        for it in br.ok:
            items_by_slug[it["slug"]] = it
            c = int(it.get("confidence", 3))
            k = str(max(1, min(5, c)))
            distribuicao[k] = distribuicao.get(k, 0) + 1
        pl: dict[str, Any] = {
            "batch_id": br.batch_id,
            "produtos": br.expected_count,
        }
        if br.usage_prompt is not None:
            pl["prompt_tokens"] = br.usage_prompt
            prompt_total += br.usage_prompt
        if br.usage_completion is not None:
            pl["completion_tokens"] = br.usage_completion
            completion_total += br.usage_completion
        if br.usage_total is not None:
            pl["total_tokens"] = br.usage_total
            total_tokens_acc += br.usage_total
        if br.usage_prompt is None and br.usage_completion is None:
            usage_known = False
        por_lote.append(pl)

    items_sorted = sorted(items_by_slug.values(), key=lambda x: x.get("slug", ""))
    enq_slugs = {str(p.get("slug", "")).strip() for p in to_process}
    classified_slugs = {
        it["slug"]
        for it in items_sorted
        if it.get("slug") in enq_slugs and it.get("categorySlug") in allowed_slugs
    }
    missing = enq_slugs - classified_slugs
    produtos_com_erro = len(missing)
    produtos_classificados_ok = len(classified_slugs)

    review_threshold = args.review_if_below
    revisao_slugs = sorted(
        it["slug"]
        for it in items_sorted
        if it.get("slug") in enq_slugs
        and int(it.get("confidence", 0)) < review_threshold
        and it.get("categorySlug") in allowed_slugs
    )
    itens_revisao_manual = len(revisao_slugs)

    if not args.quiet:
        for it in sorted(
            (x for x in items_sorted if x.get("slug") in enq_slugs),
            key=lambda x: x.get("slug", ""),
        ):
            slug = it.get("slug", "")
            name = next(
                (str(p.get("name", "")) for p in to_process if str(p.get("slug")) == slug),
                "",
            )
            conf = int(it.get("confidence", 0))
            cat = it.get("categorySlug", "")
            path = it.get("categoryPath", "")
            tag = (
                "[REVISAR]"
                if conf < review_threshold and cat in allowed_slugs
                else "[OK]"
            )
            if slug in missing:
                print(f"[ERRO] {name or slug} ({slug}) — sem classificação válida", flush=True)
            elif cat in allowed_slugs:
                print(
                    f"{tag} {name or slug} → {cat} ({conf}/5) | {path}",
                    flush=True,
                )
            else:
                print(f"[ERRO] {name or slug} ({slug}) — categoria inválida", flush=True)

    generated_at = datetime.now(timezone.utc).isoformat()
    try:
        tax_rel = str(tree_path.relative_to(SCRIPT_DIR))
    except ValueError:
        tax_rel = str(tree_path)
    out_doc = {
        "version": 1,
        "taxonomy_source": tax_rel,
        "assignable": "leaf_slugs_only" if not args.allow_non_leaf else "leaf_and_non_leaf",
        "model": model,
        "generated_at": generated_at,
        "items": items_sorted,
    }

    args.categories_out.parent.mkdir(parents=True, exist_ok=True)
    with args.categories_out.open("w", encoding="utf-8") as f:
        json.dump(out_doc, f, ensure_ascii=False, indent=2)

    elapsed = time.perf_counter() - t0

    if not args.no_review_queue_export and itens_revisao_manual > 0:
        review_items = []
        for it in items_sorted:
            if it.get("slug") not in revisao_slugs:
                continue
            slug = it.get("slug", "")
            name = next(
                (str(p.get("name", "")) for p in to_process if str(p.get("slug")) == slug),
                "",
            )
            review_items.append(
                {
                    "slug": slug,
                    "name": name,
                    "categorySlug": it.get("categorySlug"),
                    "categoryPath": it.get("categoryPath"),
                    "confidence": it.get("confidence"),
                }
            )
        args.review_queue_out.parent.mkdir(parents=True, exist_ok=True)
        with args.review_queue_out.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "generated_at": generated_at,
                    "review_if_below": review_threshold,
                    "items": review_items,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

    report = {
        "generated_at": generated_at,
        "model": model,
        "base_url_host": host_only(base_url),
        "taxonomy_prompt_mode": tax_mode,
        "taxonomy_system_chars": len(system_content),
        "dry_run": args.dry_run,
        "produtos_carregados": produtos_carregados,
        "categorias_taxonomia_folhas": len(leaves),
        "avisos_taxonomia_slugs_fundidos": slug_warnings[:100],
        "produtos_enfileirados": produtos_enfileirados,
        "produtos_classificados_ok": produtos_classificados_ok,
        "produtos_com_erro": produtos_com_erro,
        "confianca": {
            "review_if_below": review_threshold,
            "itens_revisao_manual": itens_revisao_manual,
            "produtos_revisao_slugs": revisao_slugs,
            "distribuicao": distribuicao,
        },
        "lotes_total": len(batches),
        "tempo_total_segundos": round(elapsed, 3),
        "tokens": (
            {
                "prompt_total": prompt_total,
                "completion_total": completion_total,
                "total": total_tokens_acc,
            }
            if usage_known and (prompt_total or completion_total or total_tokens_acc)
            else {
                "prompt_total": None,
                "completion_total": None,
                "total": None,
                "nota": "API não devolveu usage em todas as respostas" if not usage_known else None,
            }
        ),
        "por_lote": por_lote,
        "erros_detalhe": all_errors[:200],
    }

    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    with args.report_out.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(
        f"\nConcluído: {produtos_classificados_ok} OK, {produtos_com_erro} erros, "
        f"{itens_revisao_manual} para revisar (confidence < {review_threshold}). "
        f"Relatório: {args.report_out}",
        flush=True,
    )


if __name__ == "__main__":
    main()