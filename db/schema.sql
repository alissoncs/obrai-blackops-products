-- importacoes: uma linha por lote importado
CREATE TABLE IF NOT EXISTS importacoes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  nome TEXT,
  parser_id TEXT NOT NULL,
  parser_label TEXT NOT NULL,
  source_filename TEXT,
  product_count INTEGER NOT NULL DEFAULT 0,
  submitted_to_obrai INTEGER NOT NULL DEFAULT 0,
  submitted_at TEXT,
  updated_at TEXT
);

-- produtos: uma linha por produto do lote (campos Product + SupplierProduct do Prisma)
CREATE TABLE IF NOT EXISTS produtos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  importacao_id INTEGER NOT NULL REFERENCES importacoes(id) ON DELETE CASCADE,
  row_index INTEGER NOT NULL,

  -- Product (Prisma)
  sku TEXT,
  ean TEXT,
  friendly_id TEXT,
  slug TEXT,
  name TEXT NOT NULL,
  description TEXT,
  rich_description TEXT,
  main_image TEXT,
  additional_images TEXT,
  brand_id TEXT,
  primary_category_id TEXT,
  price_type_id TEXT,
  tags TEXT,

  -- SupplierProduct (Prisma)
  supplier_branch_id TEXT,
  retail_price REAL,
  wholesale_price REAL,
  minimum_wholesale_quantity INTEGER,
  supplier_description TEXT,
  supplier_rich_description TEXT,
  stock_quantity INTEGER NOT NULL DEFAULT 0,

  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_produtos_importacao_id ON produtos(importacao_id);
