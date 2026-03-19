"""
SQLite: conexão e criação das tabelas.
Arquivo: data/obrai.db
"""

from __future__ import annotations

from pathlib import Path

import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "obrai.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def get_db_path() -> Path:
    return DB_PATH


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    conn = get_connection()
    try:
        conn.executescript(schema)
        conn.commit()
        # Migração: coluna updated_at em importacoes (bancos antigos)
        try:
            conn.execute("ALTER TABLE importacoes ADD COLUMN updated_at TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
    finally:
        conn.close()
