# deca-import

Importação de produtos **Deca** da [Loja Dexco](https://www.lojadexco.com.br/deca/) (VTEX).

## Como funciona

- **Catálogo:** leitura via API pública VTEX em `dexcoprod.vtexcommercestable.com.br` com filtro de marca **Deca** (`fq=B:1564921765`), paginada (`_from` / `_to`, máx. 50 itens por pedido).
- **Loja pública:** `https://www.lojadexco.com.br/deca/{linkText}/p` é gravado em `sourceUrl` (o hostname da API não é o link do cliente).
- **Imagens:** todas as URLs de `items[].images[].imageUrl` de todos os SKUs do produto (sem duplicar).
- **Descrição:** `description` + `metaTagDescription` do produto; especificações em `attributes[]` a partir de `allSpecifications`.

O site `www.lojadexco.com.br` pode responder **403 / Access Denied** a bots (Akamai) em alguns IPs; a API `vtexcommercestable` costuma ser a rota estável para o scraper.

## Política de SKU (padrão vs `--per-sku`)

| Modo | `slug` | `sku` | Linhas no JSON |
|------|--------|-------|----------------|
| Padrão (por PDP) | `linkText` VTEX | `referenceId` do 1.º item, senão `itemId` | 1 por produto |
| `--per-sku` | `{linkText}-item-{itemId}` | `referenceId` ou `itemId` | 1 por SKU |

Imagens no modo padrão: **união** de todas as imagens de todos os itens.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Não é obrigatório Playwright (este scraper usa só `httpx`).

## Uso

```bash
python scraper_deca_produtos.py --limit 20
python scraper_deca_produtos.py -v
python scraper_deca_produtos.py --no-download-images
python scraper_deca_produtos.py --per-sku --limit 50
```

Merge incremental com `output/deca_products.json` por omissão; `--no-skip-existing` substitui o ficheiro só com esta execução.

**Paginação / rate limit:** `--page-delay` (segundos entre páginas da API, default 1.5) reduz risco de HTTP 429.

## Saídas

| Caminho | Conteúdo |
|---------|----------|
| `output/deca_products.json` | `{ "version": 1, "products": [...] }` alinhado ao bulk Obrai |
| `output/aux/images/products/<slug>/` | `main.*`, `02.*`, … |
| `output/aux/relatorio_<UTC>.json` | Resumo e erros |

## Push para produção

Ver **[PUSH_TO_PRODUCTION.md](PUSH_TO_PRODUCTION.md)**.

## Exemplo de PDP

[deca-bacia-convencional-deca-ravena-branco-p-9-17](https://www.lojadexco.com.br/deca/deca-bacia-convencional-deca-ravena-branco-p-9-17/p)
