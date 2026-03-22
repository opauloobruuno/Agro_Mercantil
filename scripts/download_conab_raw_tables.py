"""
Baixa a página de preços históricos da Conab, salva o HTML em data/raw/
e extrai todas as tabelas para CSV e JSON (pandas).

Execute a partir de qualquer diretório; os arquivos são relativos à raiz do projeto.
"""

import json
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Raiz do repositório (pasta acima de scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# URL configurável para inspeção
URL = "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"

# Arquivos de saída (data/raw conforme README do projeto)
PASTA_RAW = PROJECT_ROOT / "data" / "raw"
ARQUIVO_HTML = PASTA_RAW / "conab_page.html"
ARQUIVO_CSV = PASTA_RAW / "conab_raw.csv"
ARQUIVO_JSON = PASTA_RAW / "conab_raw.json"


def limpar_texto(texto):
    """Remove espaços extras e normaliza texto."""
    if texto is None:
        return ""
    return " ".join(str(texto).split()).strip()


def criar_pasta_saida():
    """Cria a pasta raw se ela ainda não existir."""
    PASTA_RAW.mkdir(parents=True, exist_ok=True)


def baixar_html(url):
    """Baixa o HTML da página com requests."""
    print(f"Baixando HTML de: {url}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response


def salvar_html(response):
    """Salva o HTML bruto no disco."""
    encoding = response.encoding or "utf-8"
    ARQUIVO_HTML.write_text(response.text, encoding=encoding, errors="replace")
    print(f"HTML salvo em: {ARQUIVO_HTML.resolve()}")


def extrair_tabelas(html):
    """Localiza tabelas, linhas e células usando BeautifulSoup."""
    soup = BeautifulSoup(html, "html.parser")
    tabelas = soup.find_all("table")

    if not tabelas:
        print("Nenhuma tabela encontrada no HTML.")
        return []

    registros = []

    for indice_tabela, tabela in enumerate(tabelas, start=1):
        print(f"Processando tabela {indice_tabela}...")

        linhas = tabela.find_all("tr")
        if not linhas:
            continue

        primeira_linha = linhas[0]
        celulas_cabecalho = primeira_linha.find_all(["th", "td"])
        cabecalhos = [limpar_texto(c.get_text(" ", strip=True)) for c in celulas_cabecalho]

        if not cabecalhos:
            continue

        for indice_linha, linha in enumerate(linhas[1:], start=1):
            celulas = linha.find_all(["td", "th"])
            valores = [limpar_texto(c.get_text(" ", strip=True)) for c in celulas]

            if not valores:
                continue

            registro = {
                "tabela_origem": f"tabela_{indice_tabela}",
                "linha_origem": indice_linha,
            }

            for posicao, coluna in enumerate(cabecalhos):
                nome_coluna = coluna or f"coluna_{posicao + 1}"
                registro[nome_coluna] = valores[posicao] if posicao < len(valores) else ""

            registros.append(registro)

    return registros


def salvar_csv_json(registros):
    """Salva os dados extraídos em CSV e JSON."""
    if not registros:
        print("Nenhum registro para salvar.")
        return None

    df = pd.DataFrame(registros)

    # Exporta raw em CSV
    df.to_csv(ARQUIVO_CSV, index=False, encoding="utf-8-sig")
    print(f"CSV salvo em: {ARQUIVO_CSV.resolve()}")

    # Exporta raw em JSON
    df.to_json(ARQUIVO_JSON, orient="records", force_ascii=False, indent=2)
    print(f"JSON salvo em: {ARQUIVO_JSON.resolve()}")

    return df


def main():
    """Executa o fluxo completo: baixar, inspecionar e exportar."""
    criar_pasta_saida()

    try:
        response = baixar_html(URL)
        print(f"Status HTTP: {response.status_code}")
        print(f"URL final: {response.url}")

        salvar_html(response)

        registros = extrair_tabelas(response.text)

        if registros:
            df = salvar_csv_json(registros)
            print(f"Total de linhas extraídas: {len(df)}")
            print("Prévia dos dados:")
            print(df.head(5).to_string(index=False))
        else:
            print("Nenhum dado estruturado foi extraído.")

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao baixar a página: {e}")
    except OSError as e:
        print(f"Erro ao salvar arquivos: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


if __name__ == "__main__":
    main()
