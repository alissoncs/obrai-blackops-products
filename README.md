# Obraí BlackOps — Importação de produtos

Ferramenta Python para **importar produtos** de múltiplos fornecedores e enviar ao marketplace **Obraí** (produção ou staging). O Obraí é um site grande e estruturado, com categorias e catálogo vindo de vários fornecedores — cada um pode mandar dados em **formato diferente**.

## Objetivo

Centralizar o fluxo: **arquivo do fornecedor → revisão humana → publicação/atualização** dos produtos no Obraí, para os clientes comprarem com dados corretos e consistentes.

## Como funciona (visão geral)

1. **Upload / seleção** — Na interface você escolhe **qual fornecedor** e **qual tipo de arquivo** está importando (ex.: PDF específico, CSV, JSON).
2. **Parser dedicado** — Cada combinação fornecedor + formato é tratada por um **script Python próprio**, capaz de ler aquele layout e normalizar os dados.
3. **Tabela no navegador** — Os dados aparecem em uma **tabela visual** para você **organizar, corrigir erros e validar** antes de enviar.
4. **Envio ao Obraí** — Ao submeter, a aplicação envia o lote para a API/backend do Obraí, que **publica ou atualiza** os produtos no site.

O processo de importação é **muito específico por fornecedor** (colunas diferentes, PDFs com layout fixo, etc.), por isso **não há um único parser genérico** — cada caso vive no seu módulo.

## Estrutura do repositório (planejada)

```
obrai-blackops-products/
├── README.md
├── example_view.png          # Captura da interface Streamlit
├── fornecedores/             # Um script (ou pacote) por fornecedor/formato
│   ├── README.md
│   ├── flank_materiais_csv.py # Flank — CSV (nome, estoque, preço)
│   └── ...
├── fixtures/                 # CSVs de exemplo para testes
├── app.py                    # Interface Streamlit (importação)
├── parsers_registry.py       # Lista de parsers para o dropdown
└── requirements.txt          # Dependências Python
```

A pasta **`fornecedores/`** concentra os **importadores customizados**: cada fornecedor (e, se preciso, cada variante de arquivo) tem sua lógica isolada, facilitando manutenção e novos parceiros.

## Exemplos de cenários

| Fornecedor | Formato        | Observação                          |
|-----------|----------------|-------------------------------------|
| A         | PDF (layout X) | Parser específico para esse PDF     |
| B         | CSV            | Mapeamento de colunas do fornecedor B |
| C         | JSON           | Estrutura própria do fornecedor C   |

A interface permite registrar **“Fornecedor B + CSV”** e acionar o parser certo.

## Requisitos

- Python 3.11+ (recomendado)

### Instalação e execução (interface Streamlit)

```bash
pip install -r requirements.txt
streamlit run app.py
```

No Windows, se `streamlit` não estiver no PATH: `py -m streamlit run app.py`

Na tela: escolha o **parser** (fornecedor/formato), envie o **CSV** e confira a tabela normalizada.

![Interface ao rodar o app — parser, upload e tabela de produtos](example_view.png)

### Parser Flank Materiais de construção

CSV com colunas **nome do produto**, **estoque** e **preço** (aceita variações de nome e formato de preço brasileiro). Ver exemplo em `fixtures/flank_exemplo.csv`.

## Contribuindo / novo fornecedor

1. Adicionar módulo em `fornecedores/` seguindo o contrato definido no `fornecedores/README.md`.
2. Registrar o fornecedor e o tipo de arquivo na UI/configuração da aplicação.
3. Testar com arquivo real e só então habilitar envio ao Obraí.

---

**Obraí** — marketplace de produtos de diversos fornecedores, com catálogo estruturado. Este repositório é a **ponte operacional** entre os arquivos brutos dos parceiros e o catálogo publicado.
