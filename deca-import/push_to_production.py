#!/usr/bin/env python3
"""
Push Deca (Loja Dexco) JSON products + local images to Obrai admin bulk import API.
See PUSH_TO_PRODUCTION.md for usage.

Brand name in Obrai admin must match PRODUCTION_BRAND_NAME (edit if needed).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import httpx
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

load_dotenv()

# --- Edit these for your production target (origin only: scheme + host, no /api path) ---
PRODUCTION_API_BASE = "https://obrai-app1-ifnsz.ondigitalocean.app"
PRODUCTION_BRAND_NAME = "Deca"

BULK_IMPORT_MAX_ITEMS = 500
BULK_IMAGE_MAX_FILES = 20
LIMIT_DEFAULT = 2
LIMIT_MAX = 100000

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)

console = Console(stderr=True)


def env_api_base() -> str:
    override = os.environ.get("OBRAI_API_BASE", "").strip()
    return override or PRODUCTION_API_BASE


def is_placeholder_category_uuid(value: str) -> bool:
    t = value.strip().lower()
    if not UUID_RE.match(t):
        return False
    return t.startswith("00000000-0000-0000-0000-")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json_products(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    products = data.get("products")
    if not isinstance(products, list):
        raise SystemExit(f'Expected top-level "products" array in {path}')
    out: list[dict[str, Any]] = []
    for i, row in enumerate(products):
        if not isinstance(row, dict):
            raise SystemExit(f"products[{i}] must be an object")
        out.append(row)
    return out


def to_bulk_row(
    raw: dict[str, Any],
    pricing_unit: str,
) -> dict[str, Any]:
    """Shape accepted by POST /api/admin/products/import (bulk v1)."""
    sku_label = str(raw.get("sku", "")).strip() or "?"

    row: dict[str, Any] = {
        "sku": raw.get("sku"),
        "name": raw.get("name"),
        "pricingUnit": pricing_unit,
    }
    for key in ("slug", "ean", "description", "richDescription", "importHash"):
        if key in raw and raw[key] is not None:
            row[key] = raw[key]

    pc = raw.get("primaryCategoryId")
    if pc is not None and pc != "":
        if not isinstance(pc, str):
            console.print(
                f"[yellow]SKU {sku_label}:[/yellow] primaryCategoryId is not a string; omitting."
            )
        else:
            t = pc.strip()
            if t:
                if not UUID_RE.match(t):
                    console.print(
                        f"[yellow]SKU {sku_label}:[/yellow] primaryCategoryId is not a valid UUID ({t!r}); omitting."
                    )
                elif is_placeholder_category_uuid(t):
                    console.print(
                        f"[yellow]SKU {sku_label}:[/yellow] omitting placeholder primaryCategoryId."
                    )
                else:
                    row["primaryCategoryId"] = t

    # Explicitly omit nested / unsupported bulk fields and image paths.
    return row


def has_json_image_association(raw: dict[str, Any]) -> bool:
    """True if JSON declares at least one image path (mainImage or images[]), without checking disk."""
    main = raw.get("mainImage")
    if isinstance(main, str) and main.strip():
        return True
    imgs = raw.get("images")
    if isinstance(imgs, list):
        for x in imgs:
            if isinstance(x, str) and x.strip():
                return True
    return False


def image_paths_for_product(
    raw: dict[str, Any],
    images_root: Path,
) -> list[Path]:
    """Ordered unique filesystem paths under images_root (max BULK_IMAGE_MAX_FILES applied by caller)."""
    seen: set[str] = set()
    ordered_rels: list[str] = []

    main = raw.get("mainImage")
    if isinstance(main, str) and main.strip():
        s = main.strip().replace("\\", "/")
        if s not in seen:
            seen.add(s)
            ordered_rels.append(s)

    imgs = raw.get("images")
    if isinstance(imgs, list):
        for x in imgs:
            if isinstance(x, str) and x.strip():
                s = x.strip().replace("\\", "/")
                if s not in seen:
                    seen.add(s)
                    ordered_rels.append(s)

    paths: list[Path] = []
    for rel in ordered_rels:
        p = (images_root / rel).resolve()
        if p.is_file():
            paths.append(p)
    return paths


def chunked(xs: list[Any], n: int) -> Iterator[list[Any]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def get_bearer_token(client: httpx.Client, base: str) -> str:
    token = os.environ.get("OBRAI_ACCESS_TOKEN", "").strip()
    if token:
        return token
    email = os.environ.get("OBRAI_ADMIN_EMAIL", "").strip()
    password = os.environ.get("OBRAI_ADMIN_PASSWORD", "").strip()
    if not email or not password:
        console.print(
            "[red]Set OBRAI_ACCESS_TOKEN or OBRAI_ADMIN_EMAIL + OBRAI_ADMIN_PASSWORD[/red]"
        )
        raise SystemExit(1)
    r = client.post(
        f"{base}/api/auth/login",
        json={"email": email, "password": password},
        timeout=60.0,
    )
    if r.status_code != 201:
        console.print(f"[red]Login failed[/red] {r.status_code} {r.text[:500]}")
        raise SystemExit(1)
    body = r.json()
    t = body.get("accessToken")
    if not t:
        console.print("[red]Login response missing accessToken[/red]")
        raise SystemExit(1)
    return str(t)


def request_with_retries(
    fn: Any,
    *,
    max_attempts: int = 4,
) -> httpx.Response:
    delay = 1.0
    last: httpx.Response | None = None
    for attempt in range(max_attempts):
        r = fn()
        last = r
        if r.status_code in (429, 502, 503, 504) and attempt < max_attempts - 1:
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
            continue
        return r
    assert last is not None
    return last


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description="Push deca_products.json to Obrai production (bulk import + images).",
    )
    p.add_argument(
        "--json-path",
        type=Path,
        default=here / "output" / "deca_products.json",
        help="Path to deca_products.json",
    )
    p.add_argument(
        "--images-root",
        type=Path,
        default=here / "output" / "aux" / "images",
        help="Root directory for image paths in JSON (e.g. .../aux/images)",
    )
    p.add_argument(
        "--state-path",
        type=Path,
        default=here / "output" / "aux" / "push_prod_state.json",
        help="JSON file tracking per-SKU progress (deca-specific; separate from other imports)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=LIMIT_DEFAULT,
        help=f"Max products to process from JSON (default {LIMIT_DEFAULT}, max {LIMIT_MAX})",
    )
    p.add_argument("--pricing-unit", default="UNIT", help='pricingUnit for new rows (default "UNIT")')
    p.add_argument(
        "--only-with-images",
        action="store_true",
        help="Process only products that have mainImage or a non-empty images[] path in JSON (then apply --limit)",
    )
    p.add_argument(
        "--skip-solutions",
        action="store_true",
        help='Exclude rows with kind=="solution" before --limit (aligns with scraper --skip-solutions)',
    )
    p.add_argument("--dry-run", action="store_true", help="No HTTP calls; no state writes")
    p.add_argument("--only-import", action="store_true", help="Skip image uploads")
    p.add_argument("--only-images", action="store_true", help="Skip JSON bulk import")
    p.add_argument("--force-import", action="store_true", help="Re-run import for SKUs even if state says done")
    p.add_argument("--force-images", action="store_true", help="Re-upload images even if state says done")
    p.add_argument("--fail-fast", action="store_true", help="Exit non-zero on first batch/SKU failure")
    p.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout seconds (default 120)",
    )
    args = p.parse_args()
    if args.limit < 1 or args.limit > LIMIT_MAX:
        p.error(f"--limit must be between 1 and {LIMIT_MAX}")
    if args.only_import and args.only_images:
        p.error("Cannot combine --only-import and --only-images")
    return args


def main() -> None:
    args = parse_args()
    base = env_api_base().rstrip("/")
    brand = PRODUCTION_BRAND_NAME.strip()
    if "example.com" in base and not os.environ.get("OBRAI_API_BASE", "").strip():
        console.print(
            "[yellow]Set PRODUCTION_API_BASE in push_to_production.py or OBRAI_API_BASE in the environment.[/yellow]"
        )

    json_path = args.json_path.resolve()
    if not json_path.is_file():
        raise SystemExit(f"Missing JSON file: {json_path}")

    images_root = args.images_root.resolve()
    state_path = args.state_path.resolve()

    all_products = load_json_products(json_path)
    if args.only_with_images:
        candidates = [p for p in all_products if has_json_image_association(p)]
        console.print(
            f"[cyan]Filter[/cyan] --only-with-images: {len(candidates)} of {len(all_products)} product(s) in JSON"
        )
    else:
        candidates = all_products

    if args.skip_solutions:
        before = len(candidates)
        candidates = [p for p in candidates if str(p.get("kind", "")).strip().lower() != "solution"]
        dropped = before - len(candidates)
        console.print(
            f"[cyan]Filter[/cyan] --skip-solutions: dropped {dropped} row(s), {len(candidates)} remaining"
        )

    raw_products = candidates[: args.limit]
    console.print(
        f"[cyan]Loaded[/cyan] {len(raw_products)} product(s) (limit={args.limit}) from {json_path.name}"
    )

    bulk_rows = [to_bulk_row(r, args.pricing_unit) for r in raw_products]
    state: dict[str, Any] = {} if args.dry_run else load_state(state_path)

    headers: dict[str, str] = {}

    with httpx.Client(timeout=args.timeout) as client:
        if not args.dry_run:
            token = get_bearer_token(client, base)
            headers["Authorization"] = f"Bearer {token}"

        # --- Phase 1: bulk JSON ---
        if not args.only_images:
            to_send: list[tuple[dict[str, Any], dict[str, Any]]] = []
            for raw, row in zip(raw_products, bulk_rows):
                sku = str(row.get("sku", "")).strip()
                if not sku:
                    continue
                st = state.get(sku)
                if (
                    not args.force_import
                    and isinstance(st, dict)
                    and st.get("import_ok")
                ):
                    continue
                to_send.append((raw, row))

            rows_only = [r for _, r in to_send]
            batches = list(chunked(rows_only, BULK_IMPORT_MAX_ITEMS))
            if args.dry_run:
                console.print(
                    f"[dry-run] Would POST {len(to_send)} product row(s) in {len(batches)} batch(es)"
                )
            else:
                total_batches = len(batches) if batches else 1
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeElapsedColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task(
                        f"Import JSON ({len(to_send)} rows)",
                        total=total_batches,
                    )
                    if not batches:
                        progress.update(task, advance=1)
                    for batch in batches:

                        def do_post() -> httpx.Response:
                            return client.post(
                                f"{base}/api/admin/products/import",
                                params={"brand": brand},
                                headers=headers,
                                json={"products": batch},
                            )

                        r = request_with_retries(do_post)
                        if r.status_code not in (200, 201):
                            console.print(
                                f"[red]Import batch failed[/red] {r.status_code} {r.text[:800]}"
                            )
                            if args.fail_fast:
                                raise SystemExit(1)
                            progress.update(task, advance=1)
                            continue
                        body = r.json()
                        failed = body.get("failed") or []
                        for frow in failed:
                            console.print(
                                f"[red]Import failed[/red] sku={frow.get('sku')} {frow.get('message')}"
                            )
                        if failed and args.fail_fast:
                            raise SystemExit(1)
                        for srow in body.get("succeeded") or []:
                            sku = str(srow.get("sku", "")).strip()
                            if not sku:
                                continue
                            if sku not in state:
                                state[sku] = {}
                            state[sku]["import_ok"] = True
                            state[sku]["import_at"] = utc_now_iso()
                            state[sku]["import_action"] = srow.get("action")
                            state[sku]["product_id"] = srow.get("productId")
                        save_state(state_path, state)
                        progress.update(task, advance=1)

        # --- Phase 2: images ---
        # Only upload for SKUs that exist in the API: Phase 1 must have set import_ok on success.
        # Otherwise the image endpoint returns "no product found with SKU...".
        if not args.only_import:
            image_jobs: list[tuple[dict[str, Any], str, list[Path]]] = []
            skipped_no_import = 0
            for raw in raw_products:
                sku = str(raw.get("sku", "")).strip()
                if not sku:
                    continue
                st = state.get(sku)
                if (
                    not args.force_images
                    and isinstance(st, dict)
                    and (st.get("images_ok") or st.get("images_skipped"))
                ):
                    continue
                if not args.dry_run:
                    if not isinstance(st, dict) or not st.get("import_ok"):
                        skipped_no_import += 1
                        continue
                paths = image_paths_for_product(raw, images_root)
                image_jobs.append((raw, sku, paths))

            if skipped_no_import:
                console.print(
                    f"[yellow]Skipped images for {skipped_no_import} SKU(s) "
                    "without a successful JSON import (run import first or fix failed rows).[/yellow]"
                )

            if args.dry_run:
                with_files = sum(1 for *_, ps in image_jobs if ps)
                console.print(
                    f"[dry-run] Would upload images for {len(image_jobs)} SKU(s) ({with_files} with local files)"
                )
            else:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeElapsedColumn(),
                    console=console,
                ) as progress:
                    img_total = len(image_jobs) if image_jobs else 1
                    task = progress.add_task("Upload images", total=img_total)
                    if not image_jobs:
                        progress.update(task, advance=1)
                    for _raw, sku, paths in image_jobs:
                        if not paths:
                            if sku not in state:
                                state[sku] = {}
                            state[sku]["images_skipped"] = True
                            state[sku]["images_reason"] = "no_local_files"
                            state[sku]["images_at"] = utc_now_iso()
                            save_state(state_path, state)
                            progress.update(task, advance=1)
                            continue

                        use_paths = paths[:BULK_IMAGE_MAX_FILES]
                        if len(paths) > BULK_IMAGE_MAX_FILES:
                            console.print(
                                f"[yellow]SKU {sku}:[/yellow] truncating gallery to {BULK_IMAGE_MAX_FILES} files"
                            )

                        def post_multipart_images() -> httpx.Response:
                            handles: list[Any] = []
                            try:
                                multipart: list[tuple[str, tuple[str, Any, str]]] = []
                                for p in use_paths:
                                    fh = open(p, "rb")
                                    handles.append(fh)
                                    multipart.append(
                                        ("files", (p.name, fh, _guess_mime(p)))
                                    )
                                return client.post(
                                    f"{base}/api/admin/products/import/images",
                                    params={"brand": brand},
                                    headers=headers,
                                    data={"sku": sku},
                                    files=multipart,  # type: ignore[arg-type]
                                )
                            finally:
                                for fh in handles:
                                    fh.close()

                        r = request_with_retries(post_multipart_images)

                        if r.status_code not in (200, 201):
                            console.print(
                                f"[red]Images failed[/red] sku={sku} {r.status_code} {r.text[:800]}"
                            )
                            if args.fail_fast:
                                raise SystemExit(1)
                            progress.update(task, advance=1)
                            continue

                        if sku not in state:
                            state[sku] = {}
                        state[sku]["images_ok"] = True
                        state[sku]["images_at"] = utc_now_iso()
                        state[sku]["images_count"] = r.json().get("imagesCount")
                        save_state(state_path, state)
                        progress.update(task, advance=1)

    console.print("[green]Done.[/green]")


def _guess_mime(path: Path) -> str:
    suf = path.suffix.lower()
    return {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".svg": "image/svg+xml",
    }.get(suf, "application/octet-stream")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("[yellow]Interrupted[/yellow]")
        sys.exit(130)
