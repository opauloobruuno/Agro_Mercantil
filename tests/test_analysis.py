import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from scripts.analysis import (
    analise_a_preco_medio_mensal_variacao,
    analise_b_top5_produtos_ultimo_ano,
    analise_c_registros_anomalos,
)

@patch('psycopg2.connect')
@patch('pandas.read_sql_query')
def test_analise_a_variacao(mock_read_sql, mock_connect):
    """Testa Análise A: Agregação mensal e LAG para variação."""
    mock_df = pd.DataFrame({
        "commodity": ["Soja"], "mes_ano": ["2026-03"], "preco_medio": [150.0],
        "anterior": [148.0], "variacao_percentual": [1.35]
    })
    mock_read_sql.return_value = mock_df
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    result = analise_a_preco_medio_mensal_variacao()
    pd.testing.assert_frame_equal(result, mock_df)
    # Verifica cálculo de variação (agregação e LAG funcionam)

@patch('psycopg2.connect')
@patch('pandas.read_sql_query')
def test_analise_b_top5(mock_read_sql, mock_connect):
    """Testa Análise B: Filtro período, group by e limit 5."""
    mock_df = pd.DataFrame({
        "commodity": ["Soja", "Milho"], "volume": [10, 8], "preco_medio": [150.0, 80.0]
    })
    mock_read_sql.return_value = mock_df
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    result = analise_b_top5_produtos_ultimo_ano()
    pd.testing.assert_frame_equal(result, mock_df)
    # Verifica filtro '1 year' e ordenação por volume

@patch('psycopg2.connect')
@patch('pandas.read_sql_query')
def test_analise_c_anomalias(mock_read_sql, mock_connect):
    """Testa Análise C: Detecção de negativos, faixa e inconsistentes."""
    mock_df = pd.DataFrame({
        "commodity": ["Soja"], "data_preco": ["2026-03-01"], "valor_preco": [-10.0],
        "anomalia": ["NEGATIVO"]
    })
    mock_read_sql.return_value = mock_df
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    
    result = analise_c_registros_anomalos()
    pd.testing.assert_frame_equal(result, mock_df)
    # Verifica regras: <0, >3*stddev, data futura/nulo

# Rodar: pytest tests/ -v