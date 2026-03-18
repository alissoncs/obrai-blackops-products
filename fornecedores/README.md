# Scripts por fornecedor

Cada fornecedor (e, quando necessário, cada **formato de arquivo**) deve ter aqui um módulo dedicado que:

1. **Lê** o arquivo (PDF, CSV, JSON, XLSX, etc.).
2. **Normaliza** os campos para o modelo de produto aceito pelo Obraí (nome, SKU, preço, categoria, estoque, imagens, etc.).
3. **Expõe** uma função ou classe que a aplicação chama após o upload, para alimentar a **tabela de revisão** no navegador.

## Convenção sugerida

- Nomeie os arquivos de forma clara, por exemplo:
  - `fornecedor_a_pdf.py`
  - `fornecedor_b_csv.py`
  - `fornecedor_c_json.py`
- Documente no topo do arquivo: layout esperado, encoding, delimitador CSV, versão do PDF, etc.

## Novo fornecedor

1. Duplique um módulo existente como referência (quando houver).
2. Implemente o parser e os testes com amostras **anonimizadas** do arquivo real.
3. Registre o par **fornecedor + tipo** na configuração da aplicação principal.

Não commite arquivos reais de fornecedores com dados sensíveis; use apenas fixtures de teste.
