import pandas as pd
import psycopg2

# Config DB (mesma do ETL)
DB_CONFIG = {
    "host": "localhost",
    "database": "agromercado_db",
    "user": "postgres",
    "password": "sua_senha",
    "port": 5432,
}


def conectar_db():
    return psycopg2.connect(**DB_CONFIG)


def analise_a_preco_medio_mensal_variacao() -> pd.DataFrame:
    """Análise A: Preço médio mensal por commodity com variação (LAG)."""
    conn = conectar_db()
    query = """
    WITH precos_mensais AS (
        SELECT c.nome AS commodity, DATE_TRUNC('month', p.data_preco) AS mes,
               ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio
        FROM precos p JOIN commodities c ON p.commodity_id = c.id
        GROUP BY c.nome, mes
    )
    SELECT commodity, TO_CHAR(mes, 'YYYY-MM') AS mes_ano, preco_medio,
           LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes) AS anterior,
           CASE WHEN LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes) IS NOT NULL
                THEN ROUND(((preco_medio - LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes)) / LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes)) * 100, 2)
                ELSE NULL END AS variacao_percentual
    FROM precos_mensais ORDER BY commodity, mes;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def analise_b_top5_produtos_ultimo_ano() -> pd.DataFrame:
    """Análise B: Top 5 produtos mais negociados (por volume de registros)."""
    conn = conectar_db()
    query = """
    SELECT c.nome AS commodity, COUNT(p.id) AS volume,
           ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio
    FROM precos p JOIN commodities c ON p.commodity_id = c.id
    WHERE p.data_preco >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY c.nome ORDER BY volume DESC LIMIT 5;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def analise_c_registros_anomalos() -> pd.DataFrame:
    """Análise C: Detecção de anomalias (negativos, fora de faixa, inconsistentes)."""
    conn = conectar_db()
    query = """
    WITH anomalias AS (
        SELECT p.id, c.nome AS commodity, p.data_preco, p.valor_preco,
               CASE WHEN p.valor_preco < 0 THEN 'NEGATIVO' END AS flag_neg,
               CASE WHEN ABS(p.valor_preco - AVG(p.valor_preco) OVER (PARTITION BY c.id)) > 3 * STDDEV(p.valor_preco) OVER (PARTITION BY c.id)
                    THEN 'FORA_FAIXA' END AS flag_faixa,
               CASE WHEN p.data_preco > CURRENT_DATE OR p.valor_preco IS NULL THEN 'INCONSISTENTE' END AS flag_inc
        FROM precos p JOIN commodities c ON p.commodity_id = c.id
    )
    SELECT commodity, data_preco, valor_preco,
           COALESCE(flag_neg, flag_faixa, flag_inc) AS anomalia
    FROM anomalias WHERE flag_neg IS NOT NULL OR flag_faixa IS NOT NULL OR flag_inc IS NOT NULL;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


if __name__ == "__main__":
    print("Análise A:", analise_a_preco_medio_mensal_variacao())
    print("Análise B:", analise_b_top5_produtos_ultimo_ano())
    print("Análise C:", analise_c_registros_anomalos())
