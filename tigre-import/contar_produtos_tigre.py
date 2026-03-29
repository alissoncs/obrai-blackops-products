#!/usr/bin/env python3
"""
Conta quantos URLs de produto existem no sitemap público da Tigre.
Usa apenas a biblioteca padrão (sem Playwright).

Uso:
  python contar_produtos_tigre.py
  python contar_produtos_tigre.py --json

Baseado na mesma URL e lógica de parse do scraper_tigre_produtos.py.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

SITEMAP_URL = "https://www.tigre.com.br/products-sitemap.xml"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_sitemap_xml(xml_bytes: bytes) -> tuple[list[str], bool]:
    """Returns (urls from <loc>, is_sitemap_index)."""
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


def _fetch(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def collect_product_urls(sitemap_url: str = SITEMAP_URL) -> list[str]:
    """Baixa o sitemap (e sub-sitemaps, se houver) e devolve URLs únicos ordenados."""
    data = _fetch(sitemap_url)
    locs, is_index = _parse_sitemap_xml(data)
    if not is_index:
        return sorted(set(locs))

    all_urls: list[str] = []
    for sub in locs:
        try:
            sub_data = _fetch(sub)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            raise RuntimeError(f"Falha ao baixar sub-sitemap {sub}: {e}") from e
        sub_locs, _ = _parse_sitemap_xml(sub_data)
        all_urls.extend(sub_locs)
    return sorted(set(all_urls))


def main() -> None:
    ap = argparse.ArgumentParser(description="Conta produtos listados no sitemap tigre.com.br")
    ap.add_argument(
        "--json",
        action="store_true",
        help="Imprime JSON {\"total\": N, \"sitemap\": \"...\"} no stdout",
    )
    ap.add_argument(
        "--url",
        default=SITEMAP_URL,
        help=f"URL do sitemap (default: {SITEMAP_URL})",
    )
    args = ap.parse_args()

    try:
        urls = collect_product_urls(args.url)
    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)

    n = len(urls)
    if args.json:
        print(json.dumps({"total": n, "sitemap": args.url}, ensure_ascii=False))
    else:
        print(n)


if __name__ == "__main__":
    main()
