# Tigre import

Scraper de produtos do site da Tigre (`scraper_tigre_produtos.py`). Plano detalhado: [`.cursor/plans/scraper-tigre-produtos.md`](.cursor/plans/scraper-tigre-produtos.md).

## Instalação

Recomendado: ambiente virtual na pasta do projeto.

```bash
cd tigre-import
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
playwright install chromium
```

## Uso

Na pasta `tigre-import/`:

```bash
# Teste rápido (10 produtos + download de imagens)
python scraper_tigre_produtos.py --limit 10 --download-images

# Só JSON, sem baixar imagens
python scraper_tigre_produtos.py --limit 50

# Ver opções
python scraper_tigre_produtos.py --help
```

## Saída

| Caminho | Conteúdo |
|---------|----------|
| `output/tigre_products.json` | Produtos no formato do import |
| `output/relatorio.json` | Totais, erros, tempo |
| `output/images/products/<slug>/` | Imagens da galeria (com `--download-images`) |

O script cria `output/` automaticamente.
