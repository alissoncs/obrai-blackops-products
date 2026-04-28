# Push Deca products to production API

Uses [`push_to_production.py`](push_to_production.py) to send `output/deca_products.json` to the Obrai admin **bulk import** endpoints (`POST /api/admin/products/import` and `POST /api/admin/products/import/images`).

## Prerequisites

- Python 3.10+ and a virtualenv (see [README.md](README.md)).
- Dependencies:

```bash
cd deca-import
source .venv/bin/activate
pip install -r requirements.txt
```

- In production, the **brand** name must match `PRODUCTION_BRAND_NAME` in `push_to_production.py` (default **`Deca`**).
- **Authentication** (one of):
  - `OBRAI_ACCESS_TOKEN`, or
  - `OBRAI_ADMIN_EMAIL` + `OBRAI_ADMIN_PASSWORD` (`POST /api/auth/login`).

Optional: `OBRAI_API_BASE` overrides the API origin (scheme + host, no trailing slash).

## Configure

1. `PRODUCTION_API_BASE` in `push_to_production.py` or env `OBRAI_API_BASE`.
2. `PRODUCTION_BRAND_NAME` if the Obrai label differs.

## Commands

Default processes only **2** products (smoke test):

```bash
cd deca-import
source .venv/bin/activate
export OBRAI_ACCESS_TOKEN="your-token"
python push_to_production.py
```

More rows:

```bash
python push_to_production.py --limit 100
```

Dry run:

```bash
python push_to_production.py --dry-run
```

Only JSON or only images:

```bash
python push_to_production.py --only-import --limit 50
python push_to_production.py --only-images --limit 50
```

Force re-import / re-upload:

```bash
python push_to_production.py --force-import --limit 10
python push_to_production.py --force-images --limit 10
```

Only rows with image paths in JSON:

```bash
python push_to_production.py --only-with-images --limit 100
```

`--skip-solutions` is accepted for parity with other importers; Deca JSON usually has no `kind` field (filter has no effect).

Custom paths:

```bash
python push_to_production.py --json-path ./output/deca_products.json \
  --images-root ./output/aux/images \
  --state-path ./output/aux/push_prod_state.json
```

```bash
python push_to_production.py --help
```

## Notes

- **Images run only after a successful JSON import** for that SKU (`import_ok` in the state file). If the bulk import failed or skipped a row, image upload is skipped for that SKU so the API does not return “no product found with SKU…”. Use `--only-import` first, then `--only-images`, or a single run that completes import before images.
- **State file** (default `output/aux/push_prod_state.json`) is **deca-specific**; use `--force-import` / `--force-images` to ignore it.
- Images are resolved under `--images-root` from `mainImage` / `images[]` in JSON.
- Bulk payload omits nested fields not supported by bulk v1 (`attributes`, `supplierProducts`, etc. are stripped in `to_bulk_row`).
- Chunks: up to **500** JSON rows per request; up to **20** files per SKU for images.

## See also

- [README.md](README.md) — scraper
- [../votoran-import/PUSH_TO_PRODUCTION.md](../votoran-import/PUSH_TO_PRODUCTION.md) — reference
