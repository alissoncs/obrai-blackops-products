# enrich-csv (Node.js CLI)

Ferramenta de linha de comando que:

1. Lê um **CSV de produtos**
2. Envia trechos ao **LM Studio** (API **OpenAI-compatible**, modelo tipo **Qwen**)
3. Grava um arquivo **JSON** ou **JSONL** com os dados enriquecidos

## Pré-requisitos

- **Node.js 18+** (usa `fetch` nativo)
- **LM Studio** com servidor local ligado (geralmente `http://localhost:1234/v1`)
- Um modelo **Qwen** carregado (ex.: `Qwen2.5-7B-Instruct` — o nome exato deve bater com o que o LM Studio expõe na API)

Guia de instalação do LM Studio no Windows: [`../docs/lm-studio-windows.md`](../docs/lm-studio-windows.md)

## Instalação

Na pasta `node/`:

```powershell
cd node
npm install
```

## Estrutura do código (responsabilidades)

| Pasta / arquivo | Papel |
|-----------------|--------|
| `cli.mjs` | Ponto de entrada: só interpreta argv e chama o pipeline. |
| `src/application/` | Orquestração do fluxo (CSV → lotes → LM Studio → disco), sem regras de domínio soltas no CLI. |
| `src/application/parseArgv.mjs` | Commander: transforma argv em objeto de configuração. |
| `src/application/enrichPipeline.mjs` | `runEnrichmentPipeline`: passos, logging com `durationMs`, escrita do output. |
| `src/domain/` | Regras de negócio: formato do CSV, schema/prompt de enriquecimento, chamada “uma leva” ao modelo. |
| `src/infrastructure/` | Detalhes técnicos: HTTP para LM Studio, Winston em `logging/`. |
| `src/config/` | Constantes de ambiente do pacote (ex.: raiz `node/` para pasta `logs/`). |
| `src/shared/` | Utilitários puros (ex.: extrair JSON da resposta do modelo). |

Fluxo resumido: **`cli` → `parseCli` → `runEnrichmentPipeline` → `domain` + `infrastructure`**.

## Configuração do LM Studio

1. Abra o LM Studio e inicie o **Local Server** (OpenAI API).
2. Confirme a URL base (padrão deste projeto: `http://localhost:1234/v1`).
3. Veja o **nome do modelo** na interface e use o mesmo em `--model`.

Variáveis de ambiente opcionais:

| Variável | Exemplo |
|----------|---------|
| `LMSTUDIO_BASE_URL` | `http://localhost:1234/v1` |
| `LMSTUDIO_MODEL` | `qwen2.5-7b-instruct` (ajuste ao seu modelo) |
| `LMSTUDIO_API_KEY` | `lm-studio` (dummy) |

## Formato do CSV de entrada

O CLI espera **exatamente** estas colunas na primeira linha (separador padrão `,`):

| Coluna | Descrição |
|--------|-----------|
| `nome do produto` | Nome / descrição do item |
| `estoque` | Quantidade em estoque |
| `preço` | Valor (pode usar vírgula decimal, ex.: `32,90`) |

Exemplo:

```csv
nome do produto,estoque,preço
Cimento CP II 50 kg,120,"32,90"
```

O arquivo é lido com `bom: true` (UTF-8 com BOM ok). Colunas com pequenas variações de nome são normalizadas antes de enviar ao modelo.

## Uso

### JSONL (recomendado para CSV grande)

```powershell
node cli.mjs -i ..\fixtures\flank_exemplo.csv -o saida.jsonl --chunk 30
```

### JSON array único

```powershell
node cli.mjs -i produtos.csv -o produtos_enriquecidos.json --format json --chunk 20
```

### CSV com separador `;`

```powershell
node cli.mjs -i planilha.csv -o saida.jsonl --delimiter ";"
```

### Schema customizado (texto livre para o prompt)

Crie um arquivo `meu_schema.txt` descrevendo os campos de saída e:

```powershell
node cli.mjs -i produtos.csv -o saida.jsonl --schema-file meu_schema.txt
```

### Opções principais

| Opção | Descrição |
|-------|-----------|
| `-i, --input` | Caminho do CSV |
| `-o, --output` | Arquivo de saída |
| `--base-url` | Base URL do LM Studio (default `http://localhost:1234/v1`) |
| `--model` | Nome do modelo Qwen carregado |
| `--chunk` | Quantas linhas por chamada ao modelo |
| `--format` | `jsonl` ou `json` |
| `--delimiter` | Separador CSV (default `,`) |

### Instalar como comando global (opcional)

```powershell
npm link
enrich-csv -i produtos.csv -o saida.jsonl
```

## Logs (Winston)

Cada execução gera um arquivo novo em **`node/logs/`** (ex.: `enrich-csv-2025-03-18T12-30-00-000Z.log`).  
Mensagens estão em **inglês**; cada etapa registra **`durationMs`** quando aplicável.

A pasta `logs/` está no `.gitignore` (raiz do repo e `node/.gitignore`).

## Notas

- O modelo deve responder **somente** com um **array JSON**; o script tenta extrair o array se vier texto extra.
- Se o chunk for muito grande, a resposta pode truncar — reduza `--chunk` ou aumente `--max-tokens` no LM Studio / CLI.
