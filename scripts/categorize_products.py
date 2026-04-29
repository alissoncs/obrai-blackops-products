#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.categorization.categories_api import (  # noqa: E402
    DEFAULT_CATEGORIES_URL,
    cache_categories_snapshot,
    fetch_categories_tree,
    flatten_level3_categories,
)
from shared.categorization.openai_classifier import OpenAiClassifier  # noqa: E402
from shared.categorization.pipeline import (  # noqa: E402
    load_products_json,
    run_stage1_pipeline,
    write_products_json,
)


DEFAULT_INPUT_BY_SUPPLIER = {
    "tigre": ROOT_DIR / "tigre-import" / "output" / "tigre_products.json",
    "deca": ROOT_DIR / "deca-import" / "output" / "deca_products.json",
    "votoran": ROOT_DIR / "votoran-import" / "output" / "votoran_products.json",
}


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def load_runtime_env() -> None:
    # Prioridade: env já exportado no shell > arquivo .env local > defaults do código.
    load_env_file(ROOT_DIR / ".env")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Categorização automática (Etapa 1): API de categorias + OpenAI + apply no JSON."
    )
    parser.add_argument(
        "--supplier",
        choices=("tigre", "deca", "votoran"),
        default="tigre",
        help="Fornecedor para resolver paths padrão de entrada/saída.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="JSON de entrada (default depende de --supplier).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="JSON de saída (default: sobrescreve --input).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Quantidade máxima de produtos para processar nesta execução.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida entradas e taxonomia sem chamar OpenAI nem gravar saída final.",
    )
    parser.add_argument(
        "--categories-url",
        default=os.environ.get("CATEGORIES_URL", DEFAULT_CATEGORIES_URL),
        help="Endpoint de categorias.",
    )
    parser.add_argument(
        "--taxonomy-cache-dir",
        type=Path,
        default=ROOT_DIR / "shared" / "categorization" / "storage" / "taxonomy_cache",
        help="Pasta para snapshot da taxonomia por execução.",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Relatório JSON da execução (default: storage/reports/...).",
    )
    parser.add_argument(
        "--openai-model",
        default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        help="Modelo OpenAI para classificação (default: gpt-4o-mini).",
    )
    parser.add_argument(
        "--openai-api-key",
        default=os.environ.get("OPENAI_API_KEY", ""),
        help="API key OpenAI (default: env OPENAI_API_KEY).",
    )
    parser.add_argument(
        "--openai-base-url",
        default=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        help="Base URL da API OpenAI-compatible.",
    )
    parser.add_argument(
        "--openai-timeout",
        type=float,
        default=float(os.environ.get("OPENAI_TIMEOUT", "45")),
        help="Timeout em segundos para chamada OpenAI.",
    )
    return parser.parse_args()


def build_report_path(cli_path: Path | None) -> Path:
    if cli_path is not None:
        return cli_path
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return ROOT_DIR / "shared" / "categorization" / "storage" / "reports" / f"run_{ts}.json"


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    input_path = args.input or DEFAULT_INPUT_BY_SUPPLIER[args.supplier]
    output_path = args.output or input_path
    return input_path, output_path


def main() -> None:
    load_runtime_env()
    args = parse_args()
    input_path, output_path = resolve_paths(args)
    report_path = build_report_path(args.report_out)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    tree = fetch_categories_tree(args.categories_url)
    snapshot_path = cache_categories_snapshot(
        tree,
        cache_dir=args.taxonomy_cache_dir,
        source_url=args.categories_url,
    )
    level3 = flatten_level3_categories(tree)
    if not level3:
        raise SystemExit("Nenhuma categoria de nível 3 ativa disponível na taxonomia")

    products = load_products_json(input_path)

    errors: list[dict[str, Any]] = []
    if args.dry_run:
        queued = len(products) if args.limit is None else min(len(products), max(0, args.limit))
        counters = {"loaded": len(products), "queued": queued, "success": 0, "failed": 0}
    else:
        if not args.openai_api_key.strip():
            raise SystemExit("Defina OPENAI_API_KEY ou passe --openai-api-key")
        classifier = OpenAiClassifier(
            api_key=args.openai_api_key,
            model=args.openai_model,
            base_url=args.openai_base_url,
            timeout_s=args.openai_timeout,
        )
        updated_products, errors, counters_obj = run_stage1_pipeline(
            products=products,
            level3_categories=level3,
            classifier=classifier,
            limit=args.limit,
            dry_run=False,
        )
        counters = {
            "loaded": counters_obj.loaded,
            "queued": counters_obj.queued,
            "success": counters_obj.success,
            "failed": counters_obj.failed,
        }
        write_products_json(output_path, updated_products)

    report = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "stage": "etapa-1",
        "supplier": args.supplier,
        "inputPath": str(input_path),
        "outputPath": str(output_path),
        "dryRun": bool(args.dry_run),
        "limit": args.limit,
        "categoriesUrl": args.categories_url,
        "taxonomySnapshotPath": str(snapshot_path),
        "level3CategoriesCount": len(level3),
        "counters": counters,
        "errors": errors[:200],
        "openaiModel": args.openai_model,
        "openaiBaseUrl": args.openai_base_url,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        (
            f"[OK] Etapa 1 finalizada | supplier={args.supplier} "
            f"| loaded={counters['loaded']} queued={counters['queued']} "
            f"| success={counters['success']} failed={counters['failed']} "
            f"| report={report_path}"
        ),
        flush=True,
    )
    if not args.dry_run:
        print(f"[OK] JSON atualizado em: {output_path}", flush=True)
    else:
        print("[INFO] Dry-run: nenhuma alteração no JSON de produtos.", flush=True)


if __name__ == "__main__":
    main()

