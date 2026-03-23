import pytest
from unittest.mock import MagicMock, patch
from etl import carregar_etl, conectar_db

@patch('psycopg2.connect')
def test_conectar_db(mock_connect):
    """Testa conexão (mock)."""
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    conn = conectar_db()
    assert conn == mock_conn

@patch('pandas.read_csv')
@patch('psycopg2.connect')
def test_etl_sucesso(mock_connect, mock_read_csv):
    """Testa ETL com dados válidos."""
    mock_df = MagicMock()
    mock_df.iterrows.return_value = [(0, {"commodity_padronizada": "Soja", "preco_limpo": 150.0, "data": "2026-03-01"})]
    mock_read_csv.return_value = mock_df
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    carregar_etl()  # Chama sem erro
    mock_cursor.execute.assert_called()  # Verifica inserção