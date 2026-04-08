#!/usr/bin/env python3
# Instalação:
#   python3 -m venv .venv && source .venv/bin/activate
#   pip install -r requirements.txt
#   playwright install chromium
#
# Uso:
#   python scraper_votoran_produtos.py --limit 10
#   python scraper_votoran_produtos.py --limit 5 -v
#   python scraper_votoran_produtos.py --no-download-images
#   python scraper_votoran_produtos.py --skip-solutions
# Retomada: por omissão faz merge com output/votoran_products.json (slugs já presentes são ignorados).
#   python scraper_votoran_produtos.py --no-skip-existing --limit 20

from __future__ import annotations

import argparse
import asyncio
import html as html_lib
import json
import os
import random
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
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

SITE_ORIGIN = "https://www.votorantimcimentos.com.br"
SITEMAP_INDEX_URL = f"{SITE_ORIGIN}/sitemap_index.xml"

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


async def _get_text_sitemaps_from_index(context: BrowserContext, *, verbose: bool) -> list[str]:
    resp: APIResponse = await context.request.get(SITEMAP_INDEX_URL, timeout=120_000)
    if not resp.ok:
        raise RuntimeError(f"Sitemap index HTTP {resp.status}: {SITEMAP_INDEX_URL}")
    locs, is_index = _parse_sitemap_xml(await resp.body())
    if not is_index:
        return [SITEMAP_INDEX_URL]
    out = [u for u in locs if "page-sitemap" in u]
    if not out:
        out = locs
    _vlog(verbose, f"sitemap index: {len(out)} page-related sub-sitemap(s)")
    return out


async def get_produtos_urls_from_wp_sitemap(context: BrowserContext, *, verbose: bool) -> list[str]:
    """URLs em /produtos/... a partir do Yoast page-sitemap (inclui categorias; filtramos na scrape)."""
    submaps = await _get_text_sitemaps_from_index(context, verbose=verbose)
    all_urls: list[str] = []
    for sm in submaps:
        _vlog(verbose, f"GET sitemap: {sm}")
        sub_resp = await context.request.get(sm, timeout=120_000)
        if not sub_resp.ok:
            _vlog(verbose, f"  skip HTTP {sub_resp.status}: {sm}")
            continue
        locs, _ = _parse_sitemap_xml(await sub_resp.body())
        all_urls.extend(locs)

    base = urlparse(SITE_ORIGIN).netloc
    seen: set[str] = set()
    out: list[str] = []
    hub_paths = {"/produtos", "/produtos/"}
    for u in sorted(set(all_urls)):
        p = urlparse(u)
        if p.netloc != base:
            continue
        path = (p.path or "").rstrip("/") + "/"
        if not path.startswith("/produtos/"):
            continue
        if path.rstrip("/") in {"/produtos"}:
            continue
        norm = f"{SITE_ORIGIN}{path}"
        if norm not in seen:
            seen.add(norm)
            out.append(norm)
    _vlog(verbose, f"unique /produtos/ URLs from sitemap: {len(out)}")
    return out


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    last = path.split("/")[-1] if path else ""
    return last.lower() or "item"


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


def _clean_text(s: str) -> str:
    t = re.sub(r"[ \t]+", " ", (s or "").replace("\r\n", "\n").replace("\r", "\n"))
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _infer_kind(
    *,
    url: str,
    tag_line: str,
    breadcrumb_labels: list[str],
    intro: str,
) -> str:
    blob = " ".join([url, tag_line, intro, *breadcrumb_labels]).lower()
    if "solução" in blob or "solucao" in blob or "/solucoes/" in blob:
        return "solution"
    return "product"


async def scrape_single_page(
    page: Page, url: str, *, verbose: bool = False
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        _vlog(verbose, f"goto: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        await page.wait_for_timeout(random.randint(400, 900))
        try:
            await page.wait_for_selector("section.cimento", timeout=15_000)
        except Error:
            _vlog(verbose, "  no section.cimento — category or non-PDP, skip")
            return None, "not_pdp"

        canonical = await page.locator('link[rel="canonical"]').first.get_attribute("href")
        source_url = _clean_text(canonical or page.url or url)

        payload = await page.evaluate(
            """() => {
          const root = document.querySelector('section.cimento');
          if (!root) return null;

          const tagEl = root.querySelector('.infos .tag');
          const tagLine = tagEl ? tagEl.innerText.trim() : '';

          let h1 = '';
          const h1El = root.querySelector('.infos h1');
          if (h1El) h1 = h1El.innerText.trim();

          const introEl = root.querySelector('.infos > p');
          const intro = introEl ? introEl.innerText.trim() : '';

          const crumbs = [];
          document.querySelectorAll('section.breadcrumbs a').forEach(a => {
            const t = a.innerText.trim();
            if (t) crumbs.push(t);
          });

          const imageUrls = [];
          const addUrl = (u) => {
            if (!u || typeof u !== 'string') return;
            let x = u.trim();
            if (x.startsWith('//')) x = 'https:' + x;
            if (!x.startsWith('http')) return;
            if (imageUrls.indexOf(x) === -1) imageUrls.push(x);
          };

          const mainEl = root.querySelector('.images .main');
          if (mainEl) {
            const bg = mainEl.style.backgroundImage || window.getComputedStyle(mainEl).backgroundImage || '';
            const m = bg.match(/url\\(['"]?([^'")]+)['"]?\\)/);
            if (m) addUrl(m[1]);
          }
          root.querySelectorAll('.images .grid .image[data-image]').forEach(el => {
            addUrl(el.getAttribute('data-image'));
          });
          root.querySelectorAll('.slider-mobile .image[data-image]').forEach(el => {
            addUrl(el.getAttribute('data-image'));
          });

          const descParts = [];
          if (tagLine) descParts.push(tagLine);
          if (intro) descParts.push(intro);

          root.querySelectorAll('.tab-contents').forEach(tab => {
            const key = tab.getAttribute('data-tab') || '';
            const label = ({ sobre: 'Sobre', caracteristicas: 'Características',
              downloads: 'Downloads', tabela: 'Tabela' })[key] || key;
            let text = tab.innerText || '';
            text = text.replace(/\\s+/g, ' ').trim();
            const links = [];
            tab.querySelectorAll('a[href]').forEach(a => {
              const href = a.getAttribute('href');
              const lt = (a.innerText || '').replace(/\\s+/g, ' ').trim();
              if (href && lt && !href.startsWith('#')) links.push(lt + ': ' + href);
            });
            if (text) descParts.push('--- ' + label + ' ---\\n' + text);
            if (links.length) descParts.push('--- ' + label + ' (links) ---\\n' + links.join('\\n'));
          });

          const attributes = [];
          const charTab = root.querySelector('.tab-contents[data-tab="caracteristicas"]');
          if (charTab) {
            charTab.querySelectorAll('.feature').forEach(f => {
              const h = f.querySelector('h4');
              const title = h ? h.innerText.trim() : '';
              const p = f.querySelector('p');
              let val = p ? p.innerText.trim() : '';
              const bar = f.querySelector('.feature-bar .bar');
              if (bar) {
                const w = bar.style.width || '';
                const subs = Array.from(f.querySelectorAll('.bar-subtitles span'))
                  .map(s => s.innerText.trim()).filter(Boolean);
                const extra = [w ? 'indicador: ' + w : '', subs.length ? subs.join(' | ') : '']
                  .filter(Boolean).join('; ');
                if (extra) val = val ? val + '\\n' + extra : extra;
              }
              if (title && val) attributes.push({ attributeKey: title, value: val });
              else if (title && !val) {
                const barOnly = f.querySelector('.feature-bar');
                if (barOnly) {
                  const bar = f.querySelector('.feature-bar .bar');
                  const w = bar ? (bar.style.width || '') : '';
                  const subs = Array.from(f.querySelectorAll('.bar-subtitles span'))
                    .map(s => s.innerText.trim()).filter(Boolean);
                  const v = [w ? 'indicador: ' + w : '', subs.join(' | ')].filter(Boolean).join('; ');
                  if (v) attributes.push({ attributeKey: title, value: v });
                }
              }
            });
          }

          return {
            tagLine, h1, intro, crumbs, imageUrls, descParts, attributes
          };
        }"""
        )

        if not payload:
            return None, "not_pdp"

        name = _clean_text(html_lib.unescape(str(payload.get("h1") or "")))
        if not name:
            try:
                title = await page.title()
                name = _clean_text(html_lib.unescape(title.split("|")[0]))
            except Error:
                name = slug_from_url(source_url)

        tag_line = _clean_text(html_lib.unescape(str(payload.get("tagLine") or "")))
        intro = _clean_text(html_lib.unescape(str(payload.get("intro") or "")))
        crumbs = payload.get("crumbs") or []
        if not isinstance(crumbs, list):
            crumbs = []
        breadcrumb_labels = [_clean_text(html_lib.unescape(str(c))) for c in crumbs if c]

        kind = _infer_kind(
            url=source_url,
            tag_line=tag_line,
            breadcrumb_labels=breadcrumb_labels,
            intro=intro,
        )

        category_path = ""
        if len(breadcrumb_labels) >= 3:
            category_path = " > ".join(breadcrumb_labels[1:-1])
        elif len(breadcrumb_labels) == 2:
            category_path = breadcrumb_labels[1]

        desc_raw = "\n\n".join(str(p) for p in (payload.get("descParts") or []) if p)
        description = _clean_text(html_lib.unescape(desc_raw))

        image_urls = payload.get("imageUrls") or []
        if not isinstance(image_urls, list):
            image_urls = []

        if not image_urls:
            try:
                og = await page.locator('meta[property="og:image"]').first.get_attribute("content")
                if og and og.strip().startswith("http"):
                    image_urls = [og.strip()]
            except Error:
                pass

        attrs_raw = payload.get("attributes") or []
        attributes: list[dict[str, str]] = []
        if isinstance(attrs_raw, list):
            for item in attrs_raw:
                if isinstance(item, dict):
                    k = _clean_text(html_lib.unescape(str(item.get("attributeKey") or "")))
                    v = _clean_text(html_lib.unescape(str(item.get("value") or "")))
                    if k and v:
                        attributes.append({"attributeKey": k, "value": v})

        slug = slug_from_url(source_url)
        body_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
        sku = ""
        for pat in (
            r"(?i)\b(?:ref|referência|referencia|c[oó]digo)\s*[:]?\s*([A-Z0-9][A-Z0-9.\-]{2,40})\b",
            r"(?i)\bSKU\s*[:]?\s*([A-Z0-9][A-Z0-9.\-]{2,40})\b",
        ):
            m = re.search(pat, body_text or "")
            if m:
                sku = _clean_text(m.group(1))
                break
        if not sku:
            sku = slug

        _vlog(
            verbose,
            f"  scraped: slug={slug!r} kind={kind} name={name[:56]!r}{'…' if len(name) > 56 else ''} "
            f"imgs={len(image_urls)} attrs={len(attributes)}",
        )

        return (
            {
                "_slug": slug,
                "_sourceUrl": source_url,
                "_name": name,
                "_sku": sku,
                "_description": description,
                "_attributes": attributes,
                "_image_urls": image_urls,
                "_kind": kind,
                "_categoryPath": category_path,
                "_tagLine": tag_line,
            },
            None,
        )
    except Exception as exc:
        _vlog(verbose, f"  scrape error: {type(exc).__name__}: {exc}")
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
            await download_image(context, u, dest)
            rel_paths.append(f"products/{slug}/{dest.name}")
            _vlog(verbose, f"    OK {dest.name} <- {u[:80]}{'…' if len(u) > 80 else ''}")
        except Exception as exc:
            errors.append({"url": u, "error": f"{type(exc).__name__}: {exc}"})
            _vlog(verbose, f"    FAIL {dest.name}: {exc}")
    return rel_paths, errors


def build_product_record(
    scraped: dict[str, Any],
    *,
    main_image: str | None,
    images: list[str] | None,
) -> dict[str, Any]:
    tags: list[str] = []
    if scraped.get("_kind") == "solution":
        tags.append("kind:solution")

    rec: dict[str, Any] = {
        "sku": scraped["_sku"] or scraped["_slug"],
        "slug": scraped["_slug"],
        "name": scraped["_name"],
        "ean": "",
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
                "retailPrice": None,
                "wholesalePrice": None,
                "minimumWholesaleQuantity": 1,
                "stockQuantity": 999,
                "status": "active",
            }
        ],
        "kind": scraped.get("_kind") or "product",
    }
    if scraped.get("_categoryPath"):
        rec["categoryPath"] = scraped["_categoryPath"]
    if tags:
        rec["tags"] = tags
    return rec


async def main_async(args: argparse.Namespace) -> int:
    v = args.verbose
    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    aux_dir = out_dir / OUTPUT_AUX_SUBDIR
    aux_dir.mkdir(parents=True, exist_ok=True)
    if args.relatorio_out is not None:
        report_path = args.relatorio_out
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = aux_dir / f"relatorio_{ts}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    images_dir = aux_dir / "images"
    products_path = out_dir / "votoran_products.json"

    _vlog(
        v,
        f"config: output_dir={out_dir} download_images={args.download_images} "
        f"skip_existing={args.skip_existing} skip_solutions={args.skip_solutions} limit={args.limit!r}",
    )
    _vlog(v, f"paths: products={products_path} images={images_dir} report={report_path}")

    started = time.perf_counter()
    report_errors: list[dict[str, str]] = []
    skipped_not_pdp: list[str] = []
    skipped_solutions: list[str] = []
    image_failures: list[dict[str, Any]] = []
    products_out: list[dict[str, Any]] = []
    urls: list[str] = []
    skipped_existing = 0
    existing_products: list[dict[str, Any]] = []
    existing_slugs: set[str] = set()

    if args.skip_existing:
        existing_products, existing_slugs = load_existing_products(products_path)
        _vlog(
            v,
            f"skip_existing: {len(existing_slugs)} slug(s) in {products_path.name}",
        )

    sem = asyncio.Semaphore(3)

    async with async_playwright() as p:
        _vlog(v, "launching chromium (headless)")
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
            urls = await get_produtos_urls_from_wp_sitemap(context, verbose=v)
            if args.limit is not None:
                urls = urls[: max(0, args.limit)]
                _vlog(v, f"after --limit={args.limit}: {len(urls)} URL(s)")

            if args.skip_existing:
                before = len(urls)
                urls = [u for u in urls if slug_from_url(u) not in existing_slugs]
                skipped_existing = before - len(urls)
                _vlog(v, f"after skip_existing: {len(urls)} to scrape, {skipped_existing} skipped")

            async def work(
                idx: int, u: str
            ) -> tuple[int, dict[str, Any] | None, dict[str, str] | None, list[str], list[str], list[dict[str, Any]]]:
                async with sem:
                    _vlog(v, f"[{idx + 1}/{len(urls)}] start {u}")
                    await asyncio.sleep(random.uniform(0.8, 2.2))
                    page = await context.new_page()
                    img_errs: list[dict[str, Any]] = []
                    not_pdp: list[str] = []
                    sol_skip: list[str] = []
                    try:
                        scraped, err = await scrape_single_page(page, u, verbose=v)
                        if err == "not_pdp":
                            not_pdp.append(u)
                            return idx, None, None, not_pdp, sol_skip, img_errs
                        if err or not scraped:
                            _vlog(v, f"[{idx + 1}/{len(urls)}] fail: {err or 'sem dados'}")
                            return idx, None, {"url": u, "mensagem": err or "sem dados"}, not_pdp, sol_skip, img_errs

                        if args.skip_solutions and scraped.get("_kind") == "solution":
                            sol_skip.append(u)
                            _vlog(v, f"[{idx + 1}/{len(urls)}] skip solution: {scraped['_slug']}")
                            return idx, None, None, not_pdp, sol_skip, img_errs

                        slug = scraped["_slug"]
                        main_rel: str | None = None
                        images_rel: list[str] | None = None

                        if args.download_images and scraped["_image_urls"]:
                            rels, dl_errs = await download_product_gallery(
                                context,
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
                                f"[{idx + 1}/{len(urls)}] skip downloads "
                                f"(download_images={args.download_images}, "
                                f"gallery_urls={len(scraped['_image_urls'])})",
                            )

                        rec = build_product_record(scraped, main_image=main_rel, images=images_rel)
                        _vlog(v, f"[{idx + 1}/{len(urls)}] ok slug={slug} kind={rec.get('kind')}")
                        return idx, rec, None, not_pdp, sol_skip, img_errs
                    finally:
                        await page.close()

            results = await tqdm_async.gather(
                *[work(i, u) for i, u in enumerate(urls)], desc="Votorantim"
            )
            for idx, rec, err, np, ss, img_e in sorted(results, key=lambda x: x[0]):
                skipped_not_pdp.extend(np)
                skipped_solutions.extend(ss)
                if err:
                    report_errors.append(err)
                elif rec:
                    products_out.append(rec)
                image_failures.extend(img_e)

        finally:
            await context.close()
            await browser.close()
            _vlog(v, "browser closed")

    elapsed = time.perf_counter() - started
    _vlog(
        v,
        f"summary: {len(products_out)} ok, {len(report_errors)} errors, "
        f"{len(skipped_not_pdp)} not_pdp, {len(skipped_solutions)} skipped solutions, "
        f"{len(image_failures)} image errors, {elapsed:.1f}s",
    )

    if args.skip_existing:
        merge_and_write_products_file(products_path, products_out, lock_dir=aux_dir)
        final_count = len(load_existing_products(products_path)[0])
        _vlog(v, f"wrote merged products: {products_path} ({final_count} total row(s))")
    else:
        write_products_json(products_path, products_out)
        final_count = len(products_out)
        _vlog(v, f"wrote products: {products_path} ({final_count} row(s))")

    report_path.write_text(
        json.dumps(
            {
                "urls_apos_limit_antes_do_skip": len(urls) + skipped_existing,
                "pulados_ja_no_arquivo": skipped_existing,
                "urls_processadas_nesta_execucao": len(urls),
                "sucesso_nesta_execucao": len(products_out),
                "erros_nesta_execucao": len(report_errors),
                "ignorados_nao_pdp": len(skipped_not_pdp),
                "ignorados_solucoes": len(skipped_solutions),
                "total_produtos_no_arquivo_final": final_count,
                "skip_existing_ativo": args.skip_existing,
                "skip_solutions_ativo": args.skip_solutions,
                "tempo_total_segundos": round(elapsed, 3),
                "erros_detalhe": report_errors,
                "urls_nao_pdp": skipped_not_pdp,
                "urls_solucoes_ignoradas": skipped_solutions,
                "imagens_falhas": image_failures,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _vlog(v, f"wrote report: {report_path}")

    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Scraper de páginas /produtos/ da Votorantim Cimentos (PDP com section.cimento). "
        "Fonte de URLs: Yoast page-sitemap. Merge incremental com votoran_products.json por omissão."
    )
    ap.add_argument("--limit", type=int, default=None, help="Processa apenas os N primeiros URLs do sitemap (após ordenação)")
    ap.add_argument(
        "--download-images",
        dest="download_images",
        action="store_true",
        help="Baixa imagens (omissão: ligado)",
    )
    ap.add_argument(
        "--no-download-images",
        dest="download_images",
        action="store_false",
        help="Não grava ficheiros de imagem",
    )
    ap.set_defaults(download_images=True)
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
        help="Substitui o JSON só com esta execução (sem merge)",
    )
    ap.add_argument(
        "--skip-solutions",
        action="store_true",
        help="Não grava registos classificados como kind=solution",
    )
    ap.add_argument(
        "--relatorio-out",
        type=Path,
        default=None,
        help="Relatório JSON (default: output/aux/relatorio_<UTC>.json)",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Logs detalhados",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
