import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from scripts.scraping import baixar_html, extrair_tabelas, padronizar_categoria, salvar_dados

@pytest.fixture
def html_mock():
    return """
    <html><table><tr><th>Commodity</th><th>Preco</th><th>Data</th></tr>
    <tr><td>soja </td><td>R$ 150,00</td><td>2026-03-01</td></tr>
    <tr><td>milho</td><td>R$ 80.50</td><td>2026-03-01</td></tr></table></html>
    """

def test_baixar_html():
    """Testa função de scraping (mock de requests)."""
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.text = "HTML teste"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        result = baixar_html("https://test.com")
        assert result == "HTML teste"

def test_extrair_tabelas_limpeza():
    """Testa extração, limpeza e padronização."""
    registros = extrair_tabelas(html_mock())
    assert len(registros) == 2
    assert registros[0]["Commodity"] == "soja"  # Limpeza de espaços
    assert registros[0]["Preco"] == "R$ 150,00"  # Preserva raw, mas limpeza em dict

def test_padronizar_categoria():
    """Testa padronização de categorias."""
    assert padronizar_categoria("SOJA", ["Soja", "Milho"]) == "Soja"
    assert padronizar_categoria("café novo", ["Café Arábica"]) == "Café Arábica"  # Matching
    assert padronizar_categoria("invalido", ["Soja"]) == "Invalido"  # Fallback

def test_salvar_dados():
    """Testa salvamento (verifica arquivos criados)."""
    dados_teste = [{"test": "data"}]
    salvar_dados(dados_teste)
    assert Path("raw/conab_raw.csv").exists()
    assert Path("raw/conab_raw.json").exists()

def test_scraping_completo_falha():
    """Testa falha em tabela vazia."""
    html_vazio = "<html></html>"
    registros = extrair_tabelas(html_vazio)
    assert len(registros) == 0