# LLM + DuckDB para consulta em linguagem natural

Este repositório demonstra como usar um LLM (OpenAI) junto com DuckDB para consultar grandes volumes de dados em linguagem natural, sem precisar escrever SQL manualmente.

Além disso, inclui um gerador de dados fake (`fake.py`) para criar datasets de teste com Faker.

---

## Estrutura do Projeto

```
.
├── main.py                 # API principal com FastAPI (upload + perguntas em linguagem natural)
├── fake.py                 # Geração de dados fake em CSV para testes locais
├── data/                   # Pasta onde o arquivo Parquet fica salvo
│   └── data.parquet
├── vendas_ecommerce_1M.zip # csv de exemplo com 1 milhão de linhas, necessário descompactar.
├── requirements.txt
├── .env                    # Variáveis de ambiente (não versionar)
└── README.md
````

---

## Pré-requisitos

- Python 3.9+
- Conta OpenAI com API Key  

Instale as dependências:

```bash
pip install -r requirements.txt
````

Crie um arquivo `.env` com:

```env
OPENAI_API_KEY=sk-xxxx
DATA_FILE=data/data.parquet   # Dataset default salvo em Parquet
OPENAI_MODEL=gpt-4o-mini
```

---

## Uso

### 1. Rodar a API

Para iniciar a API:

```bash
python main.py
```

A API ficará disponível em [http://127.0.0.1:8000](http://127.0.0.1:8000).

---

### 2. Fazer upload do CSV

Suba um dataset CSV para dentro do sistema (ele será convertido em Parquet):

```bash
curl --location 'http://localhost:8000/upload' \
--form 'file=@"vendas_ecommerce_1M.csv"'
```

Saída esperada:

```json
{
  "message": "File vendas_ecommerce_1M.csv uploaded successfully.",
  "rows": 1000000,
  "cols": 12
}
```

---

### 3. Fazer perguntas em linguagem natural

Exemplo de requisição:

```bash
curl --location 'http://localhost:8000/ask' \
--header 'Content-Type: application/json' \
--data '{
    "question": "Qual foram os totais de vendas realizadas em cada quarter em todo ano 2025?"
}'
```

Resposta esperada:

```json
{
  "question": "Qual foram os totais de vendas realizadas em cada quarter em todo ano 2025?",
  "query": "SELECT \n    EXTRACT(QUARTER FROM CAST(data_hora AS TIMESTAMP)) AS quarter,\n    SUM(quantidade * preco_unitario - desconto) AS total_vendas\nFROM \n    data\nWHERE \n    EXTRACT(YEAR FROM CAST(data_hora AS TIMESTAMP)) = 2025\nGROUP BY \n    quarter\nORDER BY \n    quarter",
  "result": [
    {
      "quarter": 1,
      "total_vendas": 1050956079.4400034
    },
    {
      "quarter": 2,
      "total_vendas": 1060384644.9000039
    },
    {
      "quarter": 3,
      "total_vendas": 746989002.1699994
    }
  ],
  "friendly_answer": "Em 2025, as vendas totais por trimestre foram as seguintes:\n\n- 1º trimestre: R$ 1.050.956.079,44\n- 2º trimestre: R$ 1.060.384.644,90\n- 3º trimestre: R$ 746.989.002,17\n\nOs dados do 4º trimestre não foram fornecidos."
}
```

---

## Observações importantes

* Esta POC **não deve ser usada em produção** sem guardrails de segurança, especialmente contra SQL injection via prompt.
* O objetivo é demonstrar que é possível trabalhar com grandes arquivos CSV/Parquet e interagir com eles em linguagem natural usando LLMs.
* Para ambientes reais, recomenda-se **adicionar validação de queries, auditoria, limites de execução e camadas de segurança**.

---

## Licença
Este projeto é apenas um exemplo educacional e pode ser adaptado livremente.