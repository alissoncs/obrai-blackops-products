# Instalar Ollama no Windows

O [Ollama](https://ollama.com) permite rodar modelos de linguagem (LLM) localmente. Este projeto usa o Ollama no script de enriquecimento de produtos (`scripts/enriquecer_importacao.py`).

## Requisitos

- **Windows 10** ou superior (recomendado Windows 10 22H2+ ou Windows 11)
- **8 GB de RAM** no mínimo (16 GB ou mais para modelos maiores)
- **5–16 GB** de espaço em disco para os modelos
- **GPU** (opcional): drivers NVIDIA 452.39+ ou AMD Radeon para aceleração

## Instalação

### Opção 1: PowerShell (recomendado)

Abra o **PowerShell** e execute:

```powershell
irm https://ollama.com/install.ps1 | iex
```

O script baixa e instala o Ollama. Não é necessário ser administrador; a instalação vai para o seu usuário.

### Opção 2: Download manual

1. Acesse **[ollama.com/download](https://ollama.com/download)** (ou [ollama.com/download/windows](https://ollama.com/download/windows)).
2. Baixe o **OllamaSetup.exe** para Windows.
3. Execute o instalador e siga as etapas.
4. Ao finalizar, o Ollama pode ser aberto pelo menu Iniciar ou pela bandeja do sistema.

## Verificar se instalou

No **PowerShell** ou no **Prompt de Comando**:

```powershell
ollama --version
```

Se aparecer a versão (ex.: `ollama version is 0.x.x`), a instalação está ok.

## Rodar o primeiro modelo

O Ollama baixa o modelo na primeira vez que você o executa. Exemplo com o Llama:

```powershell
ollama run llama3.2
```

Ou com um modelo menor:

```powershell
ollama run llama3.2:3b
```

Depois de carregar, você pode conversar no terminal. Para sair: `/bye` ou `Ctrl+D`.

## Uso com este projeto

1. Deixe o Ollama rodando (o serviço sobe automaticamente após a instalação ou ao abrir o app).
2. Rode um modelo pelo menos uma vez, por exemplo: `ollama run llama3.2`.
3. Na raiz do projeto, execute o script de enriquecimento passando o ID da importação:

```powershell
python scripts/enriquecer_importacao.py 5
```

A API do Ollama fica em **http://localhost:11434**. O script usa esse endereço por padrão.

## Links oficiais

- Site: [ollama.com](https://ollama.com)
- Download Windows: [ollama.com/download](https://ollama.com/download)
- Documentação: [github.com/ollama/ollama](https://github.com/ollama/ollama)
