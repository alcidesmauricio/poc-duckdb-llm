from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from pydantic import BaseModel
from dotenv import load_dotenv
from openai import OpenAI
import polars as pl
import duckdb
from pathlib import Path
import os
import re
import uvicorn
import logging

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dataapp")

# Basic settings
DATA_FILE = Path(os.getenv("DATA_FILE", "data/data.parquet"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("Set OPENAI_API_KEY in .env")

client = OpenAI(api_key=OPENAI_API_KEY)
app = FastAPI(title="DataApp PoC")

# ----------------------
# Utilities
# ----------------------
def ensure_data_exists() -> None:
    if not DATA_FILE.exists():
        raise HTTPException(status_code=400, detail="No data loaded yet. Please upload a file first.")

def get_schema_str() -> str:
    # Use scan to avoid loading data; read metadata only
    schema = pl.scan_parquet(DATA_FILE).schema
    return "\n".join(f"{name}: {dtype}" for name, dtype in schema.items())

def open_duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Interpolate as a string literal and escape single quotes (DuckDB uses '')
    path = str(DATA_FILE).replace("'", "''")
    con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('{path}')")
    return con

def to_records_from_relation(rel) -> list[dict]:
    # Convert to Polars then to a list of dicts
    # Avoid pandas on this path
    return rel.pl().to_dicts()

def parse_sql_from_llm(text: str) -> str:
    # Remove code fences and extract the query starting at SELECT/WITH
    sql_raw = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
    m = re.search(r"(SELECT|WITH)\s.*", sql_raw, re.IGNORECASE | re.DOTALL)
    if not m:
        raise HTTPException(status_code=400, detail=f"Could not extract SQL from LLM response. Raw: {sql_raw}")
    sql = m.group(0).strip()
    # Remove trailing ; if present
    sql = sql[:-1].strip() if sql.endswith(";") else sql
    return sql

# ----------------------
# Endpoints
# ----------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    ext = Path(file.filename).suffix.lower()
    logger.info("Uploading file: name=%s ext=%s", file.filename, ext)
    try:
        if ext == ".csv":
            # Try ';' first, fallback to ','
            try:
                df = pl.read_csv(file.file, separator=";")
            except Exception:
                file.file.seek(0)
                df = pl.read_csv(file.file)  # default separator (,)
        elif ext == ".parquet":
            df = pl.read_parquet(file.file)
        else:
            raise HTTPException(status_code=415, detail="Unsupported format. Use CSV or Parquet.")
    except Exception as e:
        logger.exception("Failed to read uploaded file")
        raise HTTPException(status_code=400, detail=f"Failed to read file: {e}")

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(DATA_FILE)
    logger.info("File saved to %s (rows=%d, cols=%d)", DATA_FILE, df.height, df.width)
    return {"message": f"File {file.filename} uploaded successfully.", "rows": df.height, "cols": df.width}

@app.get("/query")
async def query_data(q: str = Query(..., description="SQL against the 'data' VIEW")):
    ensure_data_exists()
    logger.info("Executing /query: %s", q)
    try:
        with open_duck() as con:
            rel = con.execute(q)
            result = to_records_from_relation(rel)
        logger.info("/query executed successfully: rows=%d", len(result))
        return {"query": q, "result": result}
    except Exception as e:
        logger.exception("Error executing /query")
        raise HTTPException(status_code=400, detail=f"Error executing query: {e}")

class AskBody(BaseModel):
    question: str

async def _execute_ask(question: str):
    ensure_data_exists()
    logger.info("Processing /ask question: %s", question)

    # Context: a single table/VIEW 'data'
    schema_info = get_schema_str()
    system = (
        "Você é um assistente SQL DuckDB."
        "Sempre que usar funções de data em colunas que podem ser string ou timestamp,"
        "faça cast explícito para TIMESTAMP."
        "Responda SOMENTE com uma query SQL válida (sem explicações). "
        "Apenas use a tabela/VIEW 'data'."
    )
    user_sql = f"Schema of 'data':\n{schema_info}\n\nQuestion: {question}"

    try:
        response_sql = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user_sql}],
            temperature=0
        )
        sql = parse_sql_from_llm(response_sql.choices[0].message.content or "")
        logger.info("LLM-generated SQL: %s", sql)
    except Exception as e:
        logger.exception("Failed to generate SQL via LLM")
        raise HTTPException(status_code=400, detail=f"Failed to generate SQL via LLM: {e}")

    try:
        with open_duck() as con:
            rel = con.execute(sql)
            rows = to_records_from_relation(rel)
        logger.info("Generated SQL executed successfully: rows=%d", len(rows))
    except Exception as e:
        logger.exception("Error executing LLM-generated SQL")
        raise HTTPException(status_code=400, detail=f"Error executing generated SQL: {e}\nQuery: {sql}")

    # Generate a friendly/natural-language answer
    user_friendly = (
        f"User question: {question}\n"
        f"Returned rows: {len(rows)}\n"
        f"Sample rows: {rows[:5]}"
    )
    try:
        response_friendly = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Explique o resultado de forma breve e clara em português do Brasil (pt-BR)."},
                {"role": "user", "content": user_friendly},
            ],
            temperature=0
        )
        friendly_answer = (response_friendly.choices[0].message.content or "").strip()
        logger.info("Generated friendly answer successfully")
    except Exception as e:
        logger.exception("Failed to generate friendly answer via LLM")
        raise HTTPException(status_code=400, detail=f"Failed to generate friendly answer via LLM: {e}")

    return {"question": question, "query": sql, "result": rows, "friendly_answer": friendly_answer}

@app.post("/ask")
async def ask_question_post(body: AskBody = Body(...)):
    return await _execute_ask(body.question)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)