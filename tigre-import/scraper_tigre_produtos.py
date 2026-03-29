#!/usr/bin/env python3
# Instalação:
#   python3 -m venv .venv && source .venv/bin/activate
#   pip install playwright tqdm
#   playwright install chromium
#
# Uso:
#   python scraper_tigre_produtos.py --limit 10 --download-images
# Retomada: por omissão não reprocessa slugs já em output/tigre_products.json (merge incremental).
#   python scraper_tigre_produtos.py --no-skip-existing --limit 10   # substitui o JSON inteiro

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import APIResponse, BrowserContext, Error, Page, async_playwright
from tqdm.asyncio import tqdm as tqdm_async

try:
    import fcntl  # type: ignore[attr-defined]

    _HAS_FILE_LOCK = True
except ImportError:
    _HAS_FILE_LOCK = False

SITEMAP_URL = "https://www.tigre.com.br/products-sitemap.xml"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

BRAND_ID = "00000000-0000-0000-0000-000000000000"
PRIMARY_CATEGORY_ID = "00000000-0000-0000-0000-000000000001"
SUPPLIER_BRANCH_ID = "00000000-0000-0000-0000-000000000002"


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_sitemap_xml(xml_bytes: bytes) -> tuple[list[str], bool]:
    root = ET.fromstring(xml_bytes)
    ln = _local_name(root.tag).lower()
    urls: list[str] = []
    if ln == "sitemapindex":
        for el in root:
            if _local_name(el.tag).lower() == "sitemap":
                for child in el:
                    if _local_name(child.tag).lower() == "loc" and child.text:
                        urls.append(child.text.strip())
        return urls, True
    if ln == "urlset":
        for el in root:
            if _local_name(el.tag).lower() == "url":
                for child in el:
                    if _local_name(child.tag).lower() == "loc" and child.text:
                        urls.append(child.text.strip())
        return urls, False
    return [], False


async def get_product_urls_from_sitemap(context: BrowserContext, sitemap_url: str) -> list[str]:
    resp: APIResponse = await context.request.get(sitemap_url, timeout=120_000)
    if not resp.ok:
        raise RuntimeError(f"Sitemap HTTP {resp.status}: {sitemap_url}")
    locs, is_index = _parse_sitemap_xml(await resp.body())
    if not is_index:
        return sorted(set(locs))

    all_urls: list[str] = []
    for sub in locs:
        sub_resp = await context.request.get(sub, timeout=120_000)
        if not sub_resp.ok:
            continue
        sub_locs, _ = _parse_sitemap_xml(await sub_resp.body())
        all_urls.extend(sub_locs)
    return sorted(set(all_urls))


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    last = path.split("/")[-1] if path else ""
    if last.lower().endswith(".html"):
        last = last[:-5]
    return last.lower() or "produto"


def load_existing_products(products_path: Path) -> tuple[list[dict[str, Any]], set[str]]:
    """Lê tigre_products.json e devolve (lista de produtos, slugs para skip)."""
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
    """Mantém ordem dos existentes; acrescenta slugs novos no fim; atualiza por slug se vier de novo."""
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


def merge_and_write_products_file(products_path: Path, new_products: list[dict[str, Any]]) -> None:
    """
    Re-lê o arquivo no disco, faz merge com new_products e grava.
    Com fcntl (Unix), usa lock exclusivo para reduzir corrida entre processos paralelos.
    """
    lock_path = products_path.with_suffix(products_path.suffix + ".lock")

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


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _digits_ean(s: str) -> str:
    m = re.search(r"\b(\d{8}|\d{12}|\d{13}|\d{14})\b", s)
    return m.group(1) if m else ""


def extract_ean(body_text: str, sku: str) -> str:
    """Prioriza rótulos EAN/GTIN; evita confundir SKU numérico com EAN."""
    t = body_text or ""
    for pat in (
        r"(?is)(EAN|GTIN)\s*[:]?\s*(\d{8}|\d{12}|\d{13}|\d{14})\b",
        r"(?is)(c[oó]digo\s+de\s+barras)\s*[:]?\s*(\d{8}|\d{12}|\d{13}|\d{14})\b",
    ):
        m = re.search(pat, t)
        if m:
            return m.group(2)
    cand = _digits_ean(t)
    if not cand:
        return ""
    sku_d = re.sub(r"\D", "", sku or "")
    if sku_d and cand == sku_d:
        return ""
    if sku and cand in re.sub(r"\W", "", sku):
        return ""
    return cand


def _parse_price_brl(text: str) -> float | None:
    m = re.search(r"R\$\s*([\d]{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2})", text)
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _nuxt_product_block(data: dict[str, Any], page_url: str) -> dict[str, Any] | None:
    if not data:
        return None
    slug = slug_from_url(page_url)
    key = f"page-product-{slug}"
    if key in data:
        return data[key]
    for _k, v in data.items():
        if str(_k).startswith("page-product-") and isinstance(v, dict) and "product" in v:
            return v
    return None


def _rows_to_attributes(rows: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            ks, vs = _clean_text(str(row[0])), _clean_text(str(row[1]))
            if ks and vs:
                out.append({"attributeKey": ks, "value": vs})
        elif isinstance(row, dict):
            k = row.get("label") or row.get("name")
            v = row.get("value")
            if k is not None and v is not None:
                ks, vs = _clean_text(str(k)), _clean_text(str(v))
                if ks and vs:
                    out.append({"attributeKey": ks, "value": vs})
    return out


def _specifications_to_attributes(specs: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(specs, list):
        return out
    for item in specs:
        if not isinstance(item, dict):
            continue
        label = item.get("label") or item.get("name") or item.get("title")
        value = item.get("value") or item.get("text") or item.get("description")
        if label is not None and value is not None:
            ks, vs = _clean_text(str(label)), _clean_text(str(value))
            if ks and vs:
                out.append({"attributeKey": ks, "value": vs})
    return out


async def scrape_single_product(page: Page, url: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        await page.goto(url, wait_until="networkidle", timeout=90_000)
        await page.wait_for_timeout(random.randint(500, 1500))
        await page.wait_for_timeout(5000)

        canonical = await page.locator('link[rel="canonical"]').first.get_attribute("href")
        source_url = _clean_text(canonical or page.url or url)

        nuxt = await page.evaluate("() => window.__NUXT__ && window.__NUXT__.data")
        block = _nuxt_product_block(nuxt or {}, url) if nuxt else None

        product = (block or {}).get("product") or {}
        pd = (block or {}).get("productDetail") or {}
        specs = (block or {}).get("specifications")

        name = _clean_text(str(product.get("name") or pd.get("name") or ""))
        if not name:
            try:
                h1 = await page.locator("h1.fs-36, main h1").last.inner_text(timeout=5000)
                name = _clean_text(h1)
            except Error:
                name = _clean_text((await page.title()).split("|")[0])

        sku = _clean_text(str(product.get("sku") or ""))
        if not sku:
            body_sample = await page.evaluate("() => document.body ? document.body.innerText : ''")
            m = re.search(r"CÓDIGO\s+([A-Z0-9._\-]+)", body_sample, re.I)
            if m:
                sku = _clean_text(m.group(1))

        desc = _clean_text(
            str(product.get("description") or product.get("technicalData") or product.get("informacoes_tecnicas") or "")
        )
        if not desc:
            try:
                desc = await page.locator(".page-product .tab-pane.active").first.inner_text(timeout=2000)
                desc = _clean_text(desc)
            except Error:
                pass

        body_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
        ean = extract_ean(body_text, sku)
        if not ean and isinstance(nuxt, dict):
            ean = extract_ean(json.dumps(nuxt, ensure_ascii=False), sku)

        attributes: list[dict[str, str]] = []
        attributes.extend(_specifications_to_attributes(specs))
        table = pd.get("table") or {}
        attributes.extend(_rows_to_attributes(table.get("rows")))

        dom_attrs = await page.evaluate(
            """() => {
          const out = [];
          const root = document.querySelector('.technical-info-tab')
            || document.querySelector('.page-product .technical-info-tab');
          if (!root) return out;
          root.querySelectorAll('table tr').forEach(tr => {
            const cells = tr.querySelectorAll('th,td');
            if (cells.length >= 2) {
              const k = cells[0].innerText.trim();
              const v = cells[1].innerText.trim();
              if (k && v) out.push([k, v]);
            }
          });
          return out;
        }"""
        )
        for pair in dom_attrs or []:
            if len(pair) >= 2:
                ks, vs = _clean_text(str(pair[0])), _clean_text(str(pair[1]))
                if ks and vs and not any(
                    a["attributeKey"] == ks and a["value"] == vs for a in attributes
                ):
                    attributes.append({"attributeKey": ks, "value": vs})

        retail = _parse_price_brl(body_text)

        image_urls = await page.evaluate(
            """() => {
          const out = [];
          const root = document.querySelector('.gallery-wrapper') || document.querySelector('.custom-gallery');
          if (!root) return out;
          root.querySelectorAll('img').forEach(img => {
            let u = img.currentSrc || img.src || img.getAttribute('data-src');
            if (!u || u.includes('1px.gif')) return;
            if (u.startsWith('//')) u = 'https:' + u;
            out.push(u);
          });
          return out;
        }"""
        )
        seen: set[str] = set()
        ordered: list[str] = []
        for u in image_urls or []:
            if u not in seen:
                seen.add(u)
                ordered.append(u)

        pd_image = pd.get("image")
        if isinstance(pd_image, str) and pd_image.strip().startswith("http"):
            u = pd_image.strip()
            if u not in seen:
                seen.add(u)
                ordered.insert(0, u)

        if not ordered and isinstance(product.get("image"), str) and str(product["image"]).startswith("http"):
            ordered.append(str(product["image"]).strip())

        slug = slug_from_url(source_url)

        return (
            {
                "_slug": slug,
                "_sourceUrl": source_url,
                "_name": name or slug,
                "_sku": sku or "",
                "_ean": ean or "",
                "_description": desc,
                "_retail": retail,
                "_attributes": attributes,
                "_image_urls": ordered,
            },
            None,
        )
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


async def download_image(context: BrowserContext, image_url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = await context.request.get(image_url, timeout=120_000)
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status} ao baixar imagem")
    dest.write_bytes(await resp.body())


async def download_product_gallery(
    context: BrowserContext,
    urls: list[str],
    slug: str,
    images_root: Path,
) -> tuple[list[str], list[dict[str, str]]]:
    rel_paths: list[str] = []
    errors: list[dict[str, str]] = []
    base = images_root / "products" / slug
    for i, u in enumerate(urls):
        filename = "main.jpg" if i == 0 else f"{i + 1:02d}.jpg"
        dest = base / filename
        ext = Path(urlparse(u).path).suffix.lower()
        if ext in (".png", ".webp", ".jpeg", ".jpg"):
            dest = dest.with_suffix(ext)
        try:
            await download_image(context, u, dest)
            rel_paths.append(f"products/{slug}/{dest.name}")
        except Exception as exc:
            errors.append({"url": u, "error": f"{type(exc).__name__}: {exc}"})
    return rel_paths, errors


def build_product_record(
    scraped: dict[str, Any],
    *,
    main_image: str | None,
    images: list[str] | None,
) -> dict[str, Any]:
    return {
        "sku": scraped["_sku"] or scraped["_slug"],
        "slug": scraped["_slug"],
        "name": scraped["_name"],
        "ean": scraped["_ean"] or "",
        "description": scraped["_description"] or "",
        "mainImage": main_image,
        "images": images,
        "sourceUrl": scraped["_sourceUrl"],
        "brandId": BRAND_ID,
        "primaryCategoryId": PRIMARY_CATEGORY_ID,
        "status": "active",
        "attributes": scraped["_attributes"],
        "supplierProducts": [
            {
                "supplierBranchId": SUPPLIER_BRANCH_ID,
                "retailPrice": scraped.get("_retail"),
                "wholesalePrice": None,
                "minimumWholesaleQuantity": 1,
                "stockQuantity": 999,
                "status": "active",
            }
        ],
    }


async def main_async(args: argparse.Namespace) -> int:
    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    images_dir = out_dir / "images"
    products_path = out_dir / "tigre_products.json"

    started = time.perf_counter()
    report_errors: list[dict[str, str]] = []
    image_failures: list[dict[str, Any]] = []
    products_out: list[dict[str, Any]] = []
    urls: list[str] = []
    skipped_existing = 0
    existing_products: list[dict[str, Any]] = []
    existing_slugs: set[str] = set()

    if args.skip_existing:
        existing_products, existing_slugs = load_existing_products(products_path)

    sem = asyncio.Semaphore(3)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="pt-BR",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        try:
            urls = await get_product_urls_from_sitemap(context, SITEMAP_URL)
            if args.limit is not None:
                urls = urls[: max(0, args.limit)]

            if args.skip_existing:
                before = len(urls)
                urls = [u for u in urls if slug_from_url(u) not in existing_slugs]
                skipped_existing = before - len(urls)

            async def work(
                idx: int, u: str
            ) -> tuple[int, dict[str, Any] | None, dict[str, str] | None, list[dict[str, Any]]]:
                async with sem:
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    page = await context.new_page()
                    img_errs: list[dict[str, Any]] = []
                    try:
                        scraped, err = await scrape_single_product(page, u)
                        if err or not scraped:
                            return idx, None, {"url": u, "mensagem": err or "sem dados"}, []

                        slug = scraped["_slug"]
                        main_rel: str | None = None
                        images_rel: list[str] | None = None

                        if args.download_images and scraped["_image_urls"]:
                            rels, dl_errs = await download_product_gallery(
                                context, scraped["_image_urls"], slug, images_dir
                            )
                            img_errs = [{"slug": slug, **e} for e in dl_errs]
                            if rels:
                                main_rel = rels[0]
                                images_rel = rels
                        else:
                            main_rel = None
                            images_rel = None

                        rec = build_product_record(scraped, main_image=main_rel, images=images_rel)
                        return idx, rec, None, img_errs
                    finally:
                        await page.close()

            results = await tqdm_async.gather(
                *[work(i, u) for i, u in enumerate(urls)], desc="Produtos"
            )
            for idx, rec, err, img_e in sorted(results, key=lambda x: x[0]):
                if err:
                    report_errors.append(err)
                elif rec:
                    products_out.append(rec)
                image_failures.extend(img_e)

        finally:
            await context.close()
            await browser.close()

    elapsed = time.perf_counter() - started

    if args.skip_existing:
        merge_and_write_products_file(products_path, products_out)
        final_count = len(load_existing_products(products_path)[0])
    else:
        write_products_json(products_path, products_out)
        final_count = len(products_out)

    (out_dir / "relatorio.json").write_text(
        json.dumps(
            {
                "urls_apos_limit_antes_do_skip": len(urls) + skipped_existing,
                "pulados_ja_no_arquivo": skipped_existing,
                "urls_processadas_nesta_execucao": len(urls),
                "sucesso_nesta_execucao": len(products_out),
                "erros_nesta_execucao": len(report_errors),
                "total_produtos_no_arquivo_final": final_count,
                "skip_existing_ativo": args.skip_existing,
                "tempo_total_segundos": round(elapsed, 3),
                "erros_detalhe": report_errors,
                "imagens_falhas": image_failures,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Scraper de produtos tigre.com.br. Por omissão ignora slugs já presentes em tigre_products.json (modo retomada)."
    )
    ap.add_argument("--limit", type=int, default=None, help="Processa apenas os N primeiros URLs do sitemap")
    ap.add_argument(
        "--download-images",
        action="store_true",
        help="Baixa imagens para output/images/products/{slug}/ e preenche mainImage/images",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Pasta de saída (default: output)",
    )
    ap.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        default=True,
        help="Processa todos os URLs do lote e substitui o JSON só com esta execução (sem merge)",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
