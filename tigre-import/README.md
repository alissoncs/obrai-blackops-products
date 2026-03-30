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
| `output/aux/relatorio_<UTC>.json` | Totais, erros, tempo (novo ficheiro por execução; UTC no nome) |
| `output/aux/images/products/<slug>/` | Imagens da galeria (com `--download-images`) |

O script cria `output/` e `output/aux/` automaticamente.

## Categorias (IA)

Usa o Groq para sugerir categoria por produto (lê `output/tigre_products.json` e `input/categories.json`). Copia [`.env.example`](.env.example) para `.env` e coloca a tua `GROQ_API_KEY`.

```bash
cd tigre-import
source .venv/bin/activate
python enriquecer_categorias.py --limit 50
```

Se `output/tigre_categories.json` já existir, execuções seguintes **continuam** o ficheiro e não voltam a chamar a API para slugs já classificados. Para reclassificar tudo de novo (e substituir o JSON só pelo resultado desta corrida): `python enriquecer_categorias.py --reclassify-all`. Para gravar a categoria no JSON de produtos: `python aplicar_categorias.py`.

Envia também a descrição do produto (até 400 caracteres). Se não houver descrição nem imagem, a confiança fica baixa para revisão manual.

Por defeito o prompt à API usa só a **lista de slugs** de categoria (menos tokens). Para enviar também o caminho completo (mais pesado): `--taxonomy-prompt full` ou `TIGRE_LLM_TAXONOMY_PROMPT=full` no `.env`.

Saída principal: `output/tigre_categories.json`. Relatório e exportações (`relatorio_categorias.json`, `categories_for_llm.json`, `categorias_revisao_manual.json`) em `output/aux/`.
