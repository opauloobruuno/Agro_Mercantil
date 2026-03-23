import csv
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import extras

# Configurações do Banco de Dados (ajuste para seu ambiente local/teste)
DB_CONFIG = {
    "host": "localhost",
    "database": "agromercado_db",  # Crie o DB com: CREATE DATABASE agromercado_db;
    "user": "postgres",  # Usuário padrão do PostgreSQL
    "password": "sua_senha_aqui",  # Substitua pela senha real
    "port": 5432,
}

# Configurações de arquivos
PASTA_RAW = Path("raw")
ARQUIVO_CSV_BRUTO = PASTA_RAW / "conab_raw.csv"  # CSV gerado pelo scraping
LOG_FILE = Path("logs") / "etl_load.log"

# Configuração de logging
LOG_FILE.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

def conectar_db():
    """Estabelece conexão com PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Conexão com PostgreSQL estabelecida.")
        return conn
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")
        raise

def padronizar_dados(nome: str) -> str:
    """Padroniza nomes (ex.: 'soja ' -> 'Soja')."""
    if pd.isna(nome) or not str(nome).strip():
        return ""
    return str(nome).strip().title()

def obter_ou_inserir_id(cursor, tabela: str, nome: str) -> int:
    """Insere se não existir e retorna ID (para commodities/regioes)."""
    nome_padronizado = padronizar_dados(nome)
    if not nome_padronizado:
        return None

    cursor.execute(f"SELECT id FROM agromercado.{tabela} WHERE nome ILIKE %s;", (nome_padronizado,))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute(f"INSERT INTO agromercado.{tabela} (nome) VALUES (%s) RETURNING id;", (nome_padronizado,))
    return cursor.fetchone()[0]

def registrar_carga(cursor, arquivo: str, registros: int, status: str, erro: str = None) -> int:
    """Registra a carga ETL na tabela."""
    query = """
    INSERT INTO agromercado.cargas_dados (arquivo_fonte, registros_processados, status, mensagem_erro)
    VALUES (%s, %s, %s, %s) RETURNING id;
    """
    cursor.execute(query, (arquivo, registros, status, erro))
    return cursor.fetchone()[0]

def carregar_etl():
    """Fluxo principal ETL: lê CSV bruto, normaliza e insere no SQL."""
    if not ARQUIVO_CSV_BRUTO.exists():
        logger.error(f"CSV bruto não encontrado: {ARQUIVO_CSV_BRUTO}")
        return

    conn = None
    try:
        conn = conectar_db()
        cursor = conn.cursor()

        # Registra início da carga
        carga_id = registrar_carga(cursor, ARQUIVO_CSV_BRUTO.name, 0, "EM_ANDAMENTO")
        conn.commit()
        logger.info(f"Carga iniciada com ID: {carga_id}")

        # Lê CSV com pandas
        df = pd.read_csv(ARQUIVO_CSV_BRUTO, encoding="utf-8-sig")
        logger.info(f"Lidos {len(df)} registros do CSV.")

        dados_insert = []
        erros = []
        for idx, row in df.iterrows():
            try:
                commodity_id = obter_ou_inserir_id(cursor, "commodities", row.get("commodity_nome", row.get("commodity")))
                regiao_id = obter_ou_inserir_id(cursor, "regioes", row.get("regiao_nome", row.get("regiao")))

                data_preco = pd.to_datetime(row.get("data_preco", row.get("date"))).date()
                valor_preco = pd.to_numeric(row.get("valor_preco", row.get("price")), errors="coerce")

                if commodity_id and regiao_id and pd.notna(data_preco) and pd.notna(valor_preco):
                    dados_insert.append((
                        commodity_id, regiao_id, data_preco, float(valor_preco),
                        row.get("moeda", "BRL"), row.get("url_fonte", row.get("source_url")),
                        carga_id
                    ))
                else:
                    erros.append(f"Linha {idx}: Dados inválidos - {row.to_dict()}")
                    logger.warning(f"Linha {idx} ignorada: dados incompletos.")
            except Exception as e:
                erros.append(f"Linha {idx}: {e}")
                logger.error(f"Erro na linha {idx}: {e}")

        if dados_insert:
            # Inserção em lote para eficiência
            extras.execute_values(
                cursor,
                """
                INSERT INTO agromercado.precos 
                (commodity_id, regiao_id, data_preco, valor_preco, moeda, url_fonte, carga_id)
                VALUES %s ON CONFLICT DO NOTHING;
                """,
                dados_insert,
                page_size=1000
            )
            conn.commit()
            logger.info(f"Inseridos {len(dados_insert)} registros válidos.")

        # Atualiza status da carga
        status_final = "SUCESSO" if not erros else "PARCIAL" if dados_insert else "ERRO"
        erro_msg = "; ".join(erros[:5]) if erros else None  # Limita log de erros
        cursor.execute(
            "UPDATE agromercado.cargas_dados SET registros_processados = %s, status = %s, mensagem_erro = %s WHERE id = %s;",
            (len(dados_insert), status_final, erro_msg, carga_id)
        )
        conn.commit()
        logger.info(f"Carga finalizada: {status_final} | Processados: {len(dados_insert)}")

    except Exception as e:
        logger.error(f"Erro no ETL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Conexão fechada.")

if __name__ == "__main__":
    carregar_etl()