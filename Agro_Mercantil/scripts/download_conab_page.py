# Script para baixar o HTML da página de preços agrícolas históricos da Conab
# Usando requests para fazer a requisição e salvar o conteúdo localmente

import os
import re
import requests

# URL alvo - facilmente editável
URL_ALVO = "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"


def obter_cabecalhos():
    """Retorna cabeçalhos polidos para a requisição HTTP."""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }


def sanitizar_nome_arquivo(nome: str) -> str:
    """Remove caracteres inválidos para nomes de arquivo."""
    nome = re.sub(r'[<>:/\|?*\"]', "", nome)
    nome = re.sub(r"\s+", "_", nome.strip())
    return nome[:120] if nome else "arquivo"


def baixar_pagina():
    """Baixa a página, salva o HTML e imprime informações úteis para inspeção."""
    try:
        resposta = requests.get(URL_ALVO, headers=obter_cabecalhos(), timeout=30)
        resposta.raise_for_status()

        content_type = resposta.headers.get("Content-Type", "").lower()
        if "text/html" not in content_type:
            print(f"Aviso: o conteúdo retornado não parece HTML. Content-Type: {content_type}")

        encoding = resposta.encoding or "utf-8"

        nome_arquivo = "conab_page.html"
        with open(nome_arquivo, "w", encoding=encoding, errors="replace") as arquivo:
            arquivo.write(resposta.text)

        print(f"Código de status HTTP: {resposta.status_code}")
        print(f"URL final: {resposta.url}")
        print(f"Arquivo salvo em: {os.path.abspath(nome_arquivo)}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao baixar a página: {e}")
    except OSError as e:
        print(f"Erro ao salvar o arquivo: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


if __name__ == "__main__":
    baixar_pagina()
