# converter_csv (CSV gigante -> JSON com LLM local)

Este script transforma um CSV grande em JSON usando **IA local** via um servidor **OpenAI-compatible** (recomendado: **LM Studio**).

Ele processa o CSV em **chunks**, para nao carregar tudo na RAM, e grava a saida em **JSONL** (1 objeto por linha), que funciona melhor para arquivos grandes.

## 1) O que voce precisa instalar

### LM Studio (OpenAI-compatible)
Instale/configure o LM Studio no Windows seguindo: [`docs/lm-studio-windows.md`](lm-studio-windows.md)

Depois que o servidor OpenAI-compatible estiver rodando, verifique se a API responde em:

- `http://localhost:1234/v1/chat/completions`
- `http://localhost:1234/v1/models`

Se nao existir `/v1/models` no seu caso, pelo menos confirme o `chat/completions`.

## 2) Dependencias Python

Dentro da pasta do projeto:

```powershell
pip install -r requirements.txt
```

Esse `requirements.txt` ja inclui o necessario para este script (incluindo `json-repair`).

## 3) Como rodar

### Exemplo basico (LM Studio)

```powershell
python scripts/converter_csv.py "entrada.csv" "saida.jsonl" ^
  --model Qwen2.5-7B-Instruct --chunksize 500 --base-url http://localhost:1234/v1
```

Onde:
- `entrada.csv` = caminho do seu CSV
- `saida.jsonl` = arquivo de saida (JSONL)
- `chunksize` = quantos registros sao enviados ao LLM por vez (ajuste se ficar lento)

Se o seu CSV estiver no formato brasileiro com separador `;`, use:

```powershell
python scripts/converter_csv.py "entrada.csv" "saida.jsonl" ^
  --model Qwen2.5-7B-Instruct --sep ";" --chunksize 500 --base-url http://localhost:1234/v1
```

## 4) Configurar o formato do JSON (schema)

O script ja vem com um `DEFAULT_TARGET_SCHEMA` (exemplo).

Para trocar para o seu formato, voce tem 2 opcoes:

1. **Editando o arquivo** `scripts/converter_csv.py` (mais rapido)
2. Passando um texto com o schema via:
   - `--target-schema "seu texto aqui..."`
   - ou `--target-schema-file "arquivo_com_seu_schema.txt"`

O modelo deve responder **somente** com um array JSON valido (mantendo ordem).

## Observacoes importantes (arquivos gigantes)

- A saida em JSON **array** unico fica enorme e pode travar; por isso este projeto usa **JSONL**.
- Como a transformacao depende do LLM, o tempo pode ser alto se voce tiver milhoes de linhas. Para reduzir custo/tempo:
  - diminua `chunksize` (menos risco de resposta grande)
  - ou reduza o numero de colunas enviadas ao LLM (se quiser, posso adaptar o script para selecionar colunas)

