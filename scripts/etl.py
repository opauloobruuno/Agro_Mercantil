import logging

import pandas as pd
import psycopg2
from psycopg2 import extras

try:
    from scripts.scraping import ARQUIVO_CSV
except ImportError:
    from scraping import ARQUIVO_CSV

# Config DB (ajuste para seu ambiente)
DB_CONFIG = {
    "host": "localhost",
    "database": "agromercado_db",
    "user": "postgres",
    "password": "sua_senha",
    "port": 5432,
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conectar_db() -> psycopg2.connection:
    """Conecta ao PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def carregar_etl():
    """Carrega CSV bruto para SQL (normalização e inserção)."""
    if not ARQUIVO_CSV.exists():
        logger.error("CSV bruto não encontrado.")
        return

    conn = None
    try:
        conn = conectar_db()
        cursor = conn.cursor()

        df = pd.read_csv(ARQUIVO_CSV)
        df["commodity_padronizada"] = df["commodity_padronizada"].fillna(df["commodity"]).apply(
            lambda x: x.title() if pd.notna(x) else None
        )
        df["regiao_padronizada"] = df["regiao_padronizada"].fillna(df["regiao"]).apply(
            lambda x: x.title() if pd.notna(x) else None
        )

        dados = [
            (row["commodity_padronizada"], row["preco_limpo"], row["data"])
            for _, row in df.iterrows()
            if pd.notna(row["preco_limpo"])
        ]
        extras.execute_values(
            cursor,
            "INSERT INTO agromercado.precos_raw (commodity, valor, data) VALUES %s ON CONFLICT DO NOTHING;",
            dados,
        )
        conn.commit()
        logger.info("ETL concluído: %s registros inseridos.", len(dados))

    except Exception as e:
        logger.error("Erro no ETL: %s", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    carregar_etl()
