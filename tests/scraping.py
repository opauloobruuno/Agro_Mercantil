import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Configurações
URL_CONAB = "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"  # URL base do teste
PASTA_RAW = Path("raw")
PASTA_RAW.mkdir(exist_ok=True)
ARQUIVO_HTML = PASTA_RAW / "conab_page.html"
ARQUIVO_CSV = PASTA_RAW / "conab_raw.csv"
ARQUIVO_JSON = PASTA_RAW / "conab_raw.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def baixar_html(url: str) -> str:
    """Baixa o HTML da Conab com headers para simular navegador."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    logger.info(f"HTML baixado com sucesso de {url} (status: {response.status_code})")
    return response.text

def salvar_html(html_content: str):
    """Salva o HTML bruto."""
    with open(ARQUIVO_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)
    logger.info(f"HTML salvo em {ARQUIVO_HTML}")

def extrair_tabelas(html_content: str) -> List[Dict]:
    """Extrai tabelas, linhas e células com BeautifulSoup; padroniza categorias."""
    soup = BeautifulSoup(html_content, "html.parser")
    tabelas = soup.find_all("table")
    if not tabelas:
        logger.warning("Nenhuma tabela encontrada.")
        return []

    registros = []
    for idx_tabela, tabela in enumerate(tabelas):
        cabecalhos = [th.get_text(strip=True).title() for th in tabela.find_all("th")]  # Padroniza títulos
        if not cabecalhos:
            cabecalhos = [f"coluna_{i+1}" for i in range(len(tabela.find_all("tr")[0].find_all("td")))]  # Fallback

        for linha in tabela.find_all("tr")[1:]:  # Pula cabeçalho
            celulas = [td.get_text(strip=True) for td in linha.find_all("td")]
            if len(celulas) == len(cabecalhos):
                registro = dict(zip(cabecalhos, celulas))
                # Padronização de categorias (ex.: nomes de commodities e regiões)
                if "commodity" in registro:
                    registro["commodity_padronizada"] = padronizar_categoria(registro["commodity"], categorias_validas=["Soja", "Milho", "Café Arábica", "Algodão", "Trigo"])
                if "regiao" in registro:
                    registro["regiao_padronizada"] = padronizar_categoria(registro["regiao"], categorias_validas=["Centro-Oeste", "Sudeste", "Sul", "Nordeste"])
                # Limpeza: remove valores inválidos
                if "preco" in registro:
                    try:
                        registro["preco_limpo"] = float(registro["preco"].replace(",", ".").replace("R$", ""))
                    except ValueError:
                        registro["preco_limpo"] = None
                        logger.warning(f"Preço inválido na linha: {registro['preco']}")
                registros.append(registro)

    logger.info(f"Extraídos {len(registros)} registros de {len(tabelas)} tabelas.")
    return registros

def padronizar_categoria(valor: str, categorias_validas: List[str]) -> str:
    """Padroniza nomes de categorias (ex.: matching fuzzy simples)."""
    valor_limpo = valor.strip().title()
    for cat in categorias_validas:
        if cat.lower() in valor_limpo.lower() or valor_limpo.lower() in cat.lower():
            return cat
    return valor_limpo  # Fallback

def salvar_dados(registros: List[Dict]):
    """Salva em CSV e JSON (raw)."""
    if not registros:
        logger.warning("Nenhum registro para salvar.")
        return

    df = pd.DataFrame(registros)
    df.to_csv(ARQUIVO_CSV, index=False, encoding="utf-8-sig")
    df.to_json(ARQUIVO_JSON, orient="records", indent=2, force_ascii=False)
    logger.info(f"Dados salvos: {len(df)} registros em {ARQUIVO_CSV} e {ARQUIVO_JSON}")

def executar_scraping():
    """Fluxo principal de scraping e limpeza."""
    html = baixar_html(URL_CONAB)
    salvar_html(html)
    registros = extrair_tabelas(html)
    salvar_dados(registros)

if __name__ == "__main__":
    executar_scraping()