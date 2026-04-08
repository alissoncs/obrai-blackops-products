#!/usr/bin/env python3
# Instalação:
#   python3 -m venv .venv && source .venv/bin/activate
#   pip install -r requirements.txt
#
# Uso:
#   python scraper_deca_produtos.py --limit 30
#   python scraper_deca_produtos.py -v --page-delay 2.0
#   python scraper_deca_produtos.py --no-download-images
#   python scraper_deca_produtos.py --per-sku --limit 100
# Retomada: merge com output/deca_products.json por omissão.
#   python scraper_deca_produtos.py --no-skip-existing

from __future__ import annotations

import argparse
import asyncio
import html as html_lib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from tqdm.asyncio import tqdm as tqdm_async

try:
    import fcntl  # type: ignore[attr-defined]

    _HAS_FILE_LOCK = True
except ImportError:
    _HAS_FILE_LOCK = False

# VTEX catalog (Loja Dexco / Deca) — API costuma responder mesmo quando www.lojadexco.com.br bloqueia bots.
CATALOG_API_BASE = "https://dexcoprod.vtexcommercestable.com.br"
SEARCH_PATH = "/api/catalog_system/pub/products/search"
# brandId Deca observado na API (marca "Deca")
DECA_BRAND_ID = "1564921765"
PAGE_SIZE = 50

PUBLIC_STORE_ORIGIN = "https://www.lojadexco.com.br"
STORE_DEPARTMENT = "deca"

OUTPUT_AUX_SUBDIR = "aux"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

BRAND_ID = "00000000-0000-0000-0000-000000000000"
SUPPLIER_BRANCH_ID = "00000000-0000-0000-0000-000000000002"


def _vlog(verbose: bool, msg: str) -> None:
    if verbose:
        print(f"[verbose] {msg}", flush=True)


def _clean_text(s: str) -> str:
    t = re.sub(r"[ \t]+", " ", (s or "").replace("\r\n", "\n").replace("\r", "\n"))
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _strip_html_to_text(s: str) -> str:
    if not s:
        return ""
    t = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    return _clean_text(html_lib.unescape(t))


def public_product_url(link_text: str) -> str:
    lt = (link_text or "").strip().strip("/")
    return f"{PUBLIC_STORE_ORIGIN}/{STORE_DEPARTMENT}/{lt}/p"


def load_existing_products(products_path: Path) -> tuple[list[dict[str, Any]], set[str]]:
    if not products_path.is_file():
        return [], set()
    try:
        raw = products_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        products = data.get("products") or []
        if not isinstance(products, list):
            return [], set()
        slugs: set[str] = set()
        out: list[dict[str, Any]] = []
        for p in products:
            if isinstance(p, dict):
                out.append(p)
                if p.get("slug"):
                    slugs.add(str(p["slug"]).strip().lower())
        return out, slugs
    except (json.JSONDecodeError, OSError):
        return [], set()


def merge_products(existing: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_slug: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for p in existing:
        s = (p.get("slug") or "").strip().lower()
        if not s:
            continue
        by_slug[s] = p
        order.append(s)
    for p in new:
        s = (p.get("slug") or "").strip().lower()
        if not s:
            continue
        by_slug[s] = p
        if s not in order:
            order.append(s)
    return [by_slug[s] for s in order if s in by_slug]


def write_products_json(products_path: Path, products: list[dict[str, Any]]) -> None:
    products_path.write_text(
        json.dumps({"version": 1, "products": products}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def merge_and_write_products_file(
    products_path: Path,
    new_products: list[dict[str, Any]],
    *,
    lock_dir: Path,
) -> None:
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{products_path.name}.lock"

    def _read_merge_write() -> None:
        existing, _ = load_existing_products(products_path)
        merged = merge_products(existing, new_products)
        write_products_json(products_path, merged)

    if not _HAS_FILE_LOCK:
        _read_merge_write()
        return

    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            _read_merge_write()
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def collect_image_urls_from_item(item: dict[str, Any]) -> list[str]:
    seen: list[str] = []
    found: set[str] = set()
    for im in item.get("images") or []:
        if not isinstance(im, dict):
            continue
        u = im.get("imageUrl")
        if isinstance(u, str) and u.startswith("http") and u not in found:
            found.add(u)
            seen.append(u)
    return seen


def collect_image_urls_product(p: dict[str, Any]) -> list[str]:
    seen: list[str] = []
    found: set[str] = set()
    for it in p.get("items") or []:
        if isinstance(it, dict):
            for u in collect_image_urls_from_item(it):
                if u not in found:
                    found.add(u)
                    seen.append(u)
    return seen


def specs_to_attributes(p: dict[str, Any]) -> list[dict[str, str]]:
    names = p.get("allSpecifications")
    if not isinstance(names, list):
        return []
    out: list[dict[str, str]] = []
    for name in names:
        if not isinstance(name, str) or not name.strip():
            continue
        raw_val = p.get(name)
        if raw_val is None:
            continue
        if isinstance(raw_val, list):
            val = ", ".join(_strip_html_to_text(str(x)) for x in raw_val if x is not None)
        else:
            val = _strip_html_to_text(str(raw_val))
        val = val.strip()
        if val:
            out.append({"attributeKey": name.strip(), "value": val})
    return out


def build_description(p: dict[str, Any]) -> str:
    parts: list[str] = []
    d = _clean_text(html_lib.unescape(str(p.get("description") or "")))
    m = _clean_text(html_lib.unescape(str(p.get("metaTagDescription") or "")))
    if d:
        parts.append(d)
    if m and m != d:
        parts.append(m)
    return "\n\n".join(parts)


def category_path_from_product(p: dict[str, Any]) -> str:
    cats = p.get("categories")
    if not isinstance(cats, list) or not cats:
        return ""
    first = cats[0]
    if not isinstance(first, str):
        return ""
    segs = [s for s in first.strip("/").split("/") if s]
    return " > ".join(segs)


def item_retail_price(item: dict[str, Any]) -> float | None:
    sellers = item.get("sellers")
    if not isinstance(sellers, list):
        return None
    for s in sellers:
        if not isinstance(s, dict):
            continue
        if not s.get("sellerDefault"):
            continue
        offer = s.get("commertialOffer")
        if not isinstance(offer, dict):
            continue
        price = offer.get("Price")
        if isinstance(price, (int, float)):
            return float(price)
        return None
    return None


def normalize_reference_id(ref: Any) -> str:
    """VTEX may expose referenceId as string or list of {Key, Value} (e.g. RefId)."""
    if ref is None:
        return ""
    if isinstance(ref, str):
        return ref.strip()
    if isinstance(ref, list):
        for x in ref:
            if isinstance(x, dict):
                k = str(x.get("Key") or "")
                if k.lower() in ("refid", "ref_id"):
                    return str(x.get("Value") or "").strip()
        for x in ref:
            if isinstance(x, dict) and x.get("Value") is not None:
                return str(x.get("Value") or "").strip()
        return ""
    return str(ref).strip()


def item_stock(item: dict[str, Any]) -> int:
    sellers = item.get("sellers")
    if not isinstance(sellers, list):
        return 999
    for s in sellers:
        if not isinstance(s, dict) or not s.get("sellerDefault"):
            continue
        offer = s.get("commertialOffer")
        if not isinstance(offer, dict):
            continue
        q = offer.get("AvailableQuantity")
        if isinstance(q, int) and q >= 0:
            return q
        return 999
    return 999


def vtex_product_to_scraped_rows(p: dict[str, Any], *, per_sku: bool) -> list[dict[str, Any]]:
    link_text = str(p.get("linkText") or "").strip()
    if not link_text:
        return []

    product_name = _clean_text(str(p.get("productName") or ""))
    product_ref = normalize_reference_id(p.get("productReference")) or str(
        p.get("productReferenceCode") or ""
    ).strip()
    description = build_description(p)
    attrs = specs_to_attributes(p)
    cat_path = category_path_from_product(p)
    base_url = public_product_url(link_text)

    items = [x for x in (p.get("items") or []) if isinstance(x, dict)]
    if not items:
        return []

    rows: list[dict[str, Any]] = []

    if not per_sku:
        it0 = items[0]
        sku = (
            normalize_reference_id(it0.get("referenceId"))
            or str(it0.get("itemId") or "").strip()
            or product_ref
            or link_text
        )
        ean = str(it0.get("ean") or "").strip()
        name = _clean_text(str(it0.get("nameComplete") or it0.get("name") or product_name))
        imgs = collect_image_urls_product(p)
        retail = item_retail_price(it0)
        stock = item_stock(it0)
        rows.append(
            {
                "_slug": link_text.lower(),
                "_sku": sku,
                "_ean": ean,
                "_name": name or product_name,
                "_description": description,
                "_attributes": attrs,
                "_image_urls": imgs,
                "_sourceUrl": base_url,
                "_retail": retail,
                "_stock": stock,
                "_categoryPath": cat_path,
            }
        )
        return rows

    for it in items:
        iid = str(it.get("itemId") or "").strip()
        sku = normalize_reference_id(it.get("referenceId")) or iid or product_ref or link_text
        slug = f"{link_text.lower()}-item-{iid}" if iid else link_text.lower()
        ean = str(it.get("ean") or "").strip()
        name = _clean_text(str(it.get("nameComplete") or it.get("name") or product_name))
        imgs = collect_image_urls_from_item(it)
        retail = item_retail_price(it)
        stock = item_stock(it)
        rows.append(
            {
                "_slug": slug,
                "_sku": sku,
                "_ean": ean,
                "_name": name or product_name,
                "_description": description,
                "_attributes": attrs,
                "_image_urls": imgs,
                "_sourceUrl": base_url,
                "_retail": retail,
                "_stock": stock,
                "_categoryPath": cat_path,
            }
        )
    return rows


def build_product_record(
    scraped: dict[str, Any],
    *,
    main_image: str | None,
    images: list[str] | None,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "sku": scraped["_sku"],
        "slug": scraped["_slug"],
        "name": scraped["_name"],
        "ean": scraped["_ean"] or "",
        "description": scraped["_description"] or "",
        "mainImage": main_image,
        "images": images,
        "sourceUrl": scraped["_sourceUrl"],
        "brandId": BRAND_ID,
        "primaryCategoryId": None,
        "status": "active",
        "attributes": scraped["_attributes"],
        "supplierProducts": [
            {
                "supplierBranchId": SUPPLIER_BRANCH_ID,
                "retailPrice": scraped.get("_retail"),
                "wholesalePrice": None,
                "minimumWholesaleQuantity": 1,
                "stockQuantity": scraped.get("_stock") if scraped.get("_stock") is not None else 999,
                "status": "active",
            }
        ],
    }
    if scraped.get("_categoryPath"):
        rec["categoryPath"] = scraped["_categoryPath"]
    return rec


async def download_image(client: httpx.AsyncClient, image_url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = await client.get(image_url, timeout=120.0)
    resp.raise_for_status()
    dest.write_bytes(resp.content)


async def download_product_gallery(
    client: httpx.AsyncClient,
    urls: list[str],
    slug: str,
    images_root: Path,
    *,
    verbose: bool = False,
) -> tuple[list[str], list[dict[str, str]]]:
    rel_paths: list[str] = []
    errors: list[dict[str, str]] = []
    base = images_root / "products" / slug
    _vlog(verbose, f"  download {len(urls)} image(s) -> {base}")
    for i, u in enumerate(urls):
        filename = "main.jpg" if i == 0 else f"{i + 1:02d}.jpg"
        dest = base / filename
        ext = Path(urlparse(u).path).suffix.lower()
        if ext in (".png", ".webp", ".jpeg", ".jpg", ".gif", ".svg"):
            dest = dest.with_suffix(ext)
        try:
            await download_image(client, u, dest)
            rel_paths.append(f"products/{slug}/{dest.name}")
            _vlog(verbose, f"    OK {dest.name}")
        except Exception as exc:
            errors.append({"url": u, "error": f"{type(exc).__name__}: {exc}"})
            _vlog(verbose, f"    FAIL {dest.name}: {exc}")
    return rel_paths, errors


async def fetch_search_batch(
    client: httpx.AsyncClient,
    start: int,
    *,
    page_to: int,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    params = {
        "fq": f"B:{DECA_BRAND_ID}",
        "_from": str(start),
        "_to": str(page_to),
    }
    url = f"{CATALOG_API_BASE}{SEARCH_PATH}"
    last_exc: Exception | None = None
    for attempt in range(6):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 429:
                wait = min(5.0 * (2**attempt), 90.0)
                _vlog(verbose, f"429 batch _from={start}, sleep {wait:.1f}s")
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, str):
                raise RuntimeError(f"API string response: {data[:200]}")
            if not isinstance(data, list):
                raise RuntimeError(f"Expected list, got {type(data)}")
            out: list[dict[str, Any]] = []
            for row in data:
                if isinstance(row, dict):
                    out.append(row)
            return out
        except Exception as exc:
            last_exc = exc
            wait = min(2.0 * (2**attempt), 60.0)
            _vlog(verbose, f"batch error {exc!r}, retry in {wait:.1f}s")
            await asyncio.sleep(wait)
    raise RuntimeError(f"Failed batch _from={start}: {last_exc}")


async def scrape_all_products(
    *,
    limit: int | None,
    page_delay: float,
    verbose: bool,
) -> list[dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json", "Accept-Language": "pt-BR,pt;q=0.9"}
    out: list[dict[str, Any]] = []
    start = 0
    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=120.0) as client:
        while True:
            if limit is not None:
                remain = limit - len(out)
                if remain <= 0:
                    break
                page_to = min(start + PAGE_SIZE - 1, start + remain - 1)
            else:
                page_to = start + PAGE_SIZE - 1
            batch = await fetch_search_batch(client, start, page_to=page_to, verbose=verbose)
            if not batch:
                break
            _vlog(verbose, f"page _from={start} _to={page_to} -> {len(batch)} product(s)")
            out.extend(batch)
            start += len(batch)
            if limit is not None and len(out) >= limit:
                out = out[:limit]
                break
            if len(batch) < (page_to - start + 1):
                break
            await asyncio.sleep(page_delay)
    return out


async def main_async(args: argparse.Namespace) -> int:
    v = args.verbose
    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    aux_dir = out_dir / OUTPUT_AUX_SUBDIR
    aux_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = args.relatorio_out or (aux_dir / f"relatorio_{ts}.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    images_dir = aux_dir / "images"
    products_path = out_dir / "deca_products.json"

    _vlog(v, f"catalog={CATALOG_API_BASE} brand=B:{DECA_BRAND_ID} page_delay={args.page_delay}")
    _vlog(v, f"paths: products={products_path} images={images_dir} per_sku={args.per_sku}")

    started = time.perf_counter()
    image_failures: list[dict[str, Any]] = []
    products_out: list[dict[str, Any]] = []
    report_errors: list[dict[str, str]] = []
    skipped_existing = 0
    existing_slugs: set[str] = set()

    if args.skip_existing:
        _, existing_slugs = load_existing_products(products_path)
        _vlog(v, f"skip_existing: {len(existing_slugs)} slug(s) in file")

    try:
        raw_vtex = await scrape_all_products(
            limit=args.limit,
            page_delay=args.page_delay,
            verbose=v,
        )
    except Exception as exc:
        report_errors.append({"stage": "catalog_fetch", "error": f"{type(exc).__name__}: {exc}"})
        raw_vtex = []

    scraped_rows: list[dict[str, Any]] = []
    for p in raw_vtex:
        if not isinstance(p, dict):
            continue
        scraped_rows.extend(vtex_product_to_scraped_rows(p, per_sku=args.per_sku))

    if args.skip_existing and existing_slugs:
        before = len(scraped_rows)
        scraped_rows = [r for r in scraped_rows if str(r.get("_slug", "")).strip().lower() not in existing_slugs]
        skipped_existing = before - len(scraped_rows)
        _vlog(v, f"after skip_existing: {len(scraped_rows)} to process, {skipped_existing} skipped")

    sem = asyncio.Semaphore(5)

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=120.0,
    ) as img_client:

        async def one(scraped: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
            slug = scraped["_slug"]
            img_errs: list[dict[str, Any]] = []
            async with sem:
                await asyncio.sleep(0.05)
                main_rel: str | None = None
                images_rel: list[str] | None = None
                if args.download_images and scraped["_image_urls"]:
                    rels, dl_errs = await download_product_gallery(
                        img_client,
                        scraped["_image_urls"],
                        slug,
                        images_dir,
                        verbose=v,
                    )
                    img_errs = [{"slug": slug, **e} for e in dl_errs]
                    if rels:
                        main_rel = rels[0]
                        images_rel = rels
                else:
                    _vlog(
                        v,
                        f"skip downloads slug={slug} (download_images={args.download_images}, "
                        f"n_urls={len(scraped['_image_urls'])})",
                    )
                rec = build_product_record(scraped, main_image=main_rel, images=images_rel)
                return rec, img_errs

        results = await tqdm_async.gather(
            *[one(s) for s in scraped_rows],
            desc="Deca",
        )
        for rec, img_e in results:
            if rec:
                products_out.append(rec)
            image_failures.extend(img_e)

    elapsed = time.perf_counter() - started

    if args.skip_existing:
        merge_and_write_products_file(products_path, products_out, lock_dir=aux_dir)
        final_count = len(load_existing_products(products_path)[0])
    else:
        write_products_json(products_path, products_out)
        final_count = len(products_out)

    report_path.write_text(
        json.dumps(
            {
                "vtex_products_fetched": len(raw_vtex),
                "rows_built": len(scraped_rows),
                "sucesso_nesta_execucao": len(products_out),
                "pulados_ja_no_arquivo": skipped_existing,
                "erros_catalogo": report_errors,
                "imagens_falhas": image_failures,
                "total_produtos_no_arquivo_final": final_count,
                "per_sku": args.per_sku,
                "tempo_total_segundos": round(elapsed, 3),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _vlog(v, f"wrote {products_path} ({final_count} total) report={report_path}")
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Scraper Deca (Loja Dexco) via API VTEX dexcoprod.vtexcommercestable.com.br."
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Máximo de produtos VTEX a pedir à API (paginado); omissão = catálogo completo",
    )
    ap.add_argument(
        "--page-delay",
        type=float,
        default=1.5,
        help="Segundos entre pedidos de página (reduz 429). Default 1.5",
    )
    ap.add_argument(
        "--per-sku",
        action="store_true",
        help="Uma linha JSON por SKU (item) em vez de agregar por produto",
    )
    ap.add_argument(
        "--download-images",
        dest="download_images",
        action="store_true",
    )
    ap.add_argument("--no-download-images", dest="download_images", action="store_false")
    ap.set_defaults(download_images=True)
    ap.add_argument("--output-dir", type=Path, default=Path("output"))
    ap.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        default=True,
    )
    ap.add_argument("--relatorio-out", type=Path, default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
