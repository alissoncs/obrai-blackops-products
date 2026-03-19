# Instalar e configurar LM Studio no Windows

Este tutorial prepara o **LM Studio** para funcionar como um servidor **OpenAI-compatible** local, para ser usado pelos scripts do projeto (`scripts/converter_csv.py` e `scripts/enriquecer_importacao.py`).

## 1) Instalar o LM Studio

1. Baixe o instalador para Windows em: [LM Studio Downloads](https://lmstudio.ai/download)
2. Execute o instalador.
3. Abra o **LM Studio**.

## 2) Baixar um modelo (motor de IA)

Para uso prático e relativamente “leve” em PCs comuns, uma boa escolha gratuita é:

- **Qwen2.5-7B-Instruct** (quantizações tipo `Q4_K_M` funcionam bem em CPU)

Outras opções (dependendo do seu hardware):

- Se sua máquina for mais fraca: **Qwen2.5-3B-Instruct** (menor e mais rápido)
- Se tiver GPU forte e quiser mais qualidade: **Llama 3.x Instruct** em tamanho maior (ex.: 8B / 70B, se couber)

No LM Studio:
1. Vá em **Models**.
2. Pesquise o modelo (ex.: `Qwen2.5-7B-Instruct`).
3. Faça download da quantização (ex.: `Q4_K_M`).

## 3) Rodar o servidor local (OpenAI-compatible)

1. No LM Studio, procure por algo como **Local Server** / **Start Server**.
2. Ative **OpenAI compatible** (OpenAI API compatibility).
3. Deixe a porta padrão como **1234** (é o que o projeto assume).
4. Clique em **Start Server**.

## 4) Confirmar que está funcionando

No PowerShell, rode:

```powershell
Invoke-RestMethod -Uri http://localhost:1234/v1/models
```

Se der erro, pelo menos confirme o endpoint de chat:

```powershell
Invoke-RestMethod -Uri http://localhost:1234/v1/chat/completions -Method Post -Body (@{
  model = "SEU_MODELO_AQUI"
  messages = @(
    @{ role="user"; content="teste" }
  )
  temperature = 0.1
} | ConvertTo-Json) -ContentType "application/json"
```

## 5) Qual “model” usar nos scripts

No LM Studio, ao iniciar o servidor, anote o **nome/model identifier** que aparece para a API.

Nos scripts, o parâmetro `--model` precisa bater com esse identificador.

Exemplo (o modelo exato pode variar):
```powershell
--model Qwen2.5-7B-Instruct
```

## 6) Usar com este projeto (exemplos)

### converter_csv
```powershell
python scripts/converter_csv.py "entrada.csv" "saida.jsonl" `
  --model Qwen2.5-7B-Instruct --base-url http://localhost:1234/v1 --chunksize 500
```

### enriquecer_importacao
```powershell
python scripts/enriquecer_importacao.py 5 --base-url http://localhost:1234/v1 --model Qwen2.5-7B-Instruct
```

