import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# Configurações do banco de dados
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_DATABASE = os.getenv("DB_DATABASE", "agromercado_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sua_senha")


@st.cache_data
def conectar_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None


@st.cache_data
def executar_query(query, params=None):
    conn = conectar_db()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Erro ao executar query: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def analise_a(commodity_selecionada):
    st.subheader("Análise A: Preço Médio Mensal por Commodity com Variação")
    query_a = """
    SELECT
        c.nome AS commodity,
        DATE_TRUNC('month', p.data_preco) AS mes_ano,
        ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio,
        LAG(AVG(p.valor_preco)) OVER (PARTITION BY c.nome ORDER BY DATE_TRUNC('month', p.data_preco)) AS preco_anterior,
        CASE
            WHEN LAG(AVG(p.valor_preco)) OVER (PARTITION BY c.nome ORDER BY DATE_TRUNC('month', p.data_preco)) IS NOT NULL
            THEN ROUND(((AVG(p.valor_preco) - LAG(AVG(p.valor_preco)) OVER (PARTITION BY c.nome ORDER BY DATE_TRUNC('month', p.data_preco))) / LAG(AVG(p.valor_preco)) OVER (PARTITION BY c.nome ORDER BY DATE_TRUNC('month', p.data_preco))) * 100, 2)
            ELSE NULL
        END AS variacao_percentual
    FROM precos p
    JOIN commodities c ON p.commodity_id = c.id
    WHERE c.nome = %s
    GROUP BY c.nome, DATE_TRUNC('month', p.data_preco)
    ORDER BY c.nome, mes_ano;
    """
    df_a = executar_query(query_a, (commodity_selecionada,))
    if not df_a.empty:
        fig_a = px.line(df_a, x="mes_ano", y="preco_medio", title=f"Preço Médio Mensal para {commodity_selecionada}")
        st.plotly_chart(fig_a)
        st.dataframe(df_a)
    else:
        st.write("Nenhum dado encontrado para a commodity selecionada.")


def analise_b(periodo_inicio, periodo_fim):
    st.subheader("Análise B: Top 5 Produtos Mais Negociados")
    query_b = """
    SELECT
        c.nome AS commodity,
        COUNT(p.id) AS volume_total,
        ROUND(AVG(p.valor_preco)::NUMERIC, 2) AS preco_medio
    FROM precos p
    JOIN commodities c ON p.commodity_id = c.id
    WHERE p.data_preco BETWEEN %s AND %s
    GROUP BY c.nome
    ORDER BY volume_total DESC
    LIMIT 5;
    """
    df_b = executar_query(query_b, (periodo_inicio, periodo_fim))
    if not df_b.empty:
        fig_b = px.bar(df_b, x="volume_total", y="commodity", orientation="h", title="Top 5 Commodities por Volume")
        st.plotly_chart(fig_b)
        st.dataframe(df_b)
    else:
        st.write("Nenhum dado encontrado para o período selecionado.")


def analise_c():
    st.subheader("Análise C: Registros Anômalos")
    query_c = """
    WITH anomalias AS (
        SELECT
            p.id,
            c.nome AS commodity,
            p.data_preco,
            p.valor_preco,
            CASE WHEN p.valor_preco < 0 THEN 'NEGATIVO' END AS flag_neg,
            CASE WHEN ABS(p.valor_preco - AVG(p.valor_preco) OVER (PARTITION BY c.id)) > 3 * STDDEV(p.valor_preco) OVER (PARTITION BY c.id)
                 THEN 'FORA_FAIXA' END AS flag_faixa,
            CASE WHEN p.data_preco > CURRENT_DATE OR p.valor_preco IS NULL THEN 'INCONSISTENTE' END AS flag_inc
        FROM precos p
        JOIN commodities c ON p.commodity_id = c.id
    )
    SELECT
        commodity,
        data_preco,
        valor_preco,
        COALESCE(flag_neg, flag_faixa, flag_inc) AS tipo_anomalia
    FROM anomalias
    WHERE flag_neg IS NOT NULL OR flag_faixa IS NOT NULL OR flag_inc IS NOT NULL
    ORDER BY data_preco DESC;
    """
    df_c = executar_query(query_c)
    if not df_c.empty:
        total_anomalias = len(df_c)
        total_df = executar_query("SELECT COUNT(*) AS total FROM precos;")
        total_registros = total_df.iloc[0]["total"] if not total_df.empty else 0
        percentual_limpo = round(((total_registros - total_anomalias) / total_registros * 100), 2) if total_registros > 0 else 0
        st.metric("Total de Anomalias", total_anomalias)
        st.metric("% de Dados Limpos", f"{percentual_limpo}%")

        anomalia_counts = df_c["tipo_anomalia"].value_counts().reset_index()
        anomalia_counts.columns = ["tipo_anomalia", "count"]
        fig_c = px.pie(anomalia_counts, values="count", names="tipo_anomalia", title="Distribuição de Tipos de Anomalias")
        st.plotly_chart(fig_c)
        st.dataframe(df_c)
    else:
        st.success("Nenhuma anomalia detectada nos dados!")


def main():
    st.title("Dashboard de Análises de Preços de Commodities - Conab")
    st.sidebar.header("Filtros")

    commodities = executar_query("SELECT DISTINCT c.nome AS commodity FROM commodities c JOIN precos p ON c.id = p.commodity_id;")
    if not commodities.empty:
        commodity_list = sorted(commodities["commodity"].tolist())
        commodity_selecionada = st.sidebar.selectbox("Selecione a Commodity para Análise A", commodity_list)
    else:
        commodity_selecionada = "Soja"

    periodo_fim = st.sidebar.date_input("Data Fim para Análise B", datetime.now().date())
    periodo_inicio = st.sidebar.date_input("Data Início para Análise B", (datetime.now() - timedelta(days=365)).date())

    if st.sidebar.button("Atualizar Dados"):
        st.cache_data.clear()
        st.rerun()

    col1, col2 = st.columns(2)
    with col1:
        if commodity_selecionada:
            analise_a(commodity_selecionada)
        analise_c()
    with col2:
        analise_b(periodo_inicio, periodo_fim)


if __name__ == "__main__":
    main()
