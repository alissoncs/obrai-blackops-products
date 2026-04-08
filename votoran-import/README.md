# votoran-import

Scraper para páginas de produto/solução em [votorantimcimentos.com.br/produtos](https://www.votorantimcimentos.com.br/produtos/).

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Uso

```bash
python scraper_votoran_produtos.py              # todos os URLs do page-sitemap (Yoast)
python scraper_votoran_produtos.py --limit 20
python scraper_votoran_produtos.py -v           # logs detalhados
python scraper_votoran_produtos.py --no-download-images
python scraper_votoran_produtos.py --skip-solutions   # ignora kind=solution
```

Por omissão faz **merge** com `output/votoran_products.json` (slugs já presentes não são reprocessados). Para substituir o ficheiro inteiro: `--no-skip-existing`.

## Push para produção (Obrai)

Depois do scrape, envia o JSON e imagens locais para a API bulk do admin. Ver **[PUSH_TO_PRODUCTION.md](PUSH_TO_PRODUCTION.md)** (`python push_to_production.py`, variáveis `OBRAI_ACCESS_TOKEN` ou email/senha, marca **Votorantim** em `push_to_production.py`).

## Saídas

| Ficheiro | Conteúdo |
|----------|----------|
| `output/votoran_products.json` | Lista no formato alinhado ao bulk Obrai (como `tigre-import`) + `kind` (`product` \| `solution`), `categoryPath`, `tags` opcional |
| `output/aux/images/products/<slug>/` | Imagens da galeria (hero + thumbs), extensão preservada |
| `output/aux/relatorio_<UTC>.json` | Estatísticas, URLs ignorados (categoria sem PDP), erros |

## PDP vs categoria

Só são importadas páginas com `section.cimento` (template de produto/linha com foto, abas Sobre/Características, etc.). URLs do sitemap que são apenas índices de categoria são contados em `urls_nao_pdp` no relatório.

## Classificação product / solution

Heurística simples: texto da URL, tag de destaque, introdução e breadcrumbs que mencionem “solução” → `kind: "solution"` e tag `kind:solution`. Ajustável no código (`_infer_kind`).
