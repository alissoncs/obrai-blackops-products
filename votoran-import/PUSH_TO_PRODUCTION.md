# Push Votorantim products to production API

This uses [`push_to_production.py`](push_to_production.py) to send `output/votoran_products.json` to the Obrai admin **bulk import** endpoints (`POST /api/admin/products/import` and `POST /api/admin/products/import/images`).

## Prerequisites

- Python 3.10+ and a virtualenv (see [README.md](README.md) installation).
- Install dependencies:

```bash
cd votoran-import
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

- In **production**, ensure the **brand** exists and its name matches `PRODUCTION_BRAND_NAME` in `push_to_production.py` (default **`Votorantim`**). If your admin label differs (e.g. “Votorantim Cimentos”), edit that constant.
- **Authentication** (pick one):
  - `OBRAI_ACCESS_TOKEN` — bearer token for an admin user, or
  - `OBRAI_ADMIN_EMAIL` and `OBRAI_ADMIN_PASSWORD` — the script logs in via `POST /api/auth/login`.

Optional: set `OBRAI_API_BASE` to override the API origin without editing the script (same value as `PRODUCTION_API_BASE`: scheme + host, **no** trailing slash).

## Configure the script

1. Edit **`PRODUCTION_API_BASE`** in `push_to_production.py` (or use env `OBRAI_API_BASE`).
2. Edit **`PRODUCTION_BRAND_NAME`** if your production brand label differs.

Do **not** commit real tokens; use environment variables.

## Commands

Default run processes only the **first 2 products** (safe smoke test):

```bash
cd votoran-import
source .venv/bin/activate
export OBRAI_ACCESS_TOKEN="your-token"
python push_to_production.py
```

Process more rows (`--limit` must be between **1** and **100000**):

```bash
python push_to_production.py --limit 100
```

Dry run (no HTTP, no state file updates):

```bash
python push_to_production.py --dry-run
```

Only JSON import or only images:

```bash
python push_to_production.py --only-import --limit 50
python push_to_production.py --only-images --limit 50
```

Re-run import or images even if the local state says they are done:

```bash
python push_to_production.py --force-import --limit 10
python push_to_production.py --force-images --limit 10
```

Stop on first batch or image error:

```bash
python push_to_production.py --fail-fast --limit 500
```

Only products that declare image paths in JSON (`mainImage` or non-empty `images` array); `--limit` applies **after** this filter:

```bash
python push_to_production.py --only-with-images --limit 100
```

Exclude rows with `kind: "solution"` (same idea as the scraper’s `--skip-solutions`); `--limit` applies **after** this filter:

```bash
python push_to_production.py --skip-solutions --limit 100
```

Custom paths:

```bash
python push_to_production.py --json-path ./output/votoran_products.json \
  --images-root ./output/aux/images \
  --state-path ./output/aux/push_prod_state.json
```

All options:

```bash
python push_to_production.py --help
```

## Behaviour notes

- **State file** (default `output/aux/push_prod_state.json`): **votoran-specific** — separate from `tigre-import` so SKU progress does not collide. Tracks per-SKU `import_ok` and `images_ok` / `images_skipped`; use `--force-import` / `--force-images` to ignore it.
- **Images** are read from paths in JSON relative to `--images-root` (default `output/aux/images`).
- **`--only-with-images`** keeps only rows where JSON has a non-empty `mainImage` string or at least one path in `images`; does not verify files on disk.
- **`--skip-solutions`** drops rows where `kind` is `solution` (case-insensitive).
- JSON rows are adapted for bulk v1: nested `supplierProducts` / `attributes` / `kind` / `categoryPath` / `tags` are not sent; `pricingUnit` defaults to `UNIT` (override with `--pricing-unit`); placeholder category UUIDs are omitted.
- Bulk JSON is sent in chunks of up to **500** rows per request; up to **20** image files per SKU per request.

## See also

- Scraper docs: [README.md](README.md)
- Tigre equivalent (reference): [../tigre-import/PUSH_TO_PRODUCTION.md](../tigre-import/PUSH_TO_PRODUCTION.md)
