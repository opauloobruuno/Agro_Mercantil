import json
import os
import re
from collections import defaultdict

from bs4 import BeautifulSoup

ARQUIVO_HTML = "conab_page.html"
ARQUIVO_JSON = "conab_page_inspection.json"

PALAVRAS_CHAVE = [
    "soja",
    "milho",
    "trigo",
    "algodão",
    "algodao",
    "café",
    "cafe",
    "arroz",
    "cana",
    "preço",
    "preco",
    "série",
    "serie",
    "histórico",
    "historico",
    "commodity",
    "indicador",
    "preços agropecuários",
    "precos agropecuarios",
]


def limpar_texto(texto):
    """Normaliza espaços e remove ruídos básicos."""
    if not texto:
        return ""
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def pontuar_relevancia(texto, href):
    """Pontua links com base em palavras-chave relacionadas ao tema."""
    conteudo = f"{texto} {href}".lower()
    return sum(1 for palavra in PALAVRAS_CHAVE if palavra in conteudo)


def classificar_link(texto, href):
    """Classifica o link por relevância para mapear seletores do spider."""
    score = pontuar_relevancia(texto, href)
    if score >= 2:
        return "alta"
    if score == 1:
        return "média"
    return "baixa"


def extrair_titulos(soup):
    """Extrai title, h1, h2 e h3 da página."""
    return {
        "title": limpar_texto(soup.title.get_text()) if soup.title else "",
        "h1": [limpar_texto(tag.get_text()) for tag in soup.find_all("h1") if limpar_texto(tag.get_text())],
        "h2": [limpar_texto(tag.get_text()) for tag in soup.find_all("h2") if limpar_texto(tag.get_text())],
        "h3": [limpar_texto(tag.get_text()) for tag in soup.find_all("h3") if limpar_texto(tag.get_text())],
    }


def extrair_links(soup):
    """Extrai todos os links com texto e href, além de uma classificação de relevância."""
    links = []
    for a in soup.find_all("a", href=True):
        texto = limpar_texto(a.get_text(" ", strip=True))
        href = limpar_texto(a.get("href", ""))
        score = pontuar_relevancia(texto, href)
        links.append(
            {
                "text": texto,
                "href": href,
                "score": score,
                "relevancia": classificar_link(texto, href),
            }
        )
    return links


def extrair_tabelas(soup):
    """Extrai tabelas, cabeçalhos e primeiras linhas para inspeção rápida."""
    tabelas = []

    for idx, table in enumerate(soup.find_all("table"), start=1):
        headers = []

        thead = table.find("thead")
        if thead:
            headers = [limpar_texto(th.get_text(" ", strip=True)) for th in thead.find_all("th")]
        else:
            first_row = table.find("tr")
            if first_row:
                headers = [limpar_texto(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]

        rows = []
        for tr in table.find_all("tr")[1:4]:
            cells = [limpar_texto(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)

        tabelas.append(
            {
                "index": idx,
                "headers": headers,
                "sample_rows": rows,
                "row_count": len(table.find_all("tr")),
            }
        )

    return tabelas


def extrair_formularios(soup):
    """Resume formulários, botões e inputs para ajudar a entender a página."""
    forms = []
    for form in soup.find_all("form"):
        forms.append(
            {
                "action": limpar_texto(form.get("action", "")),
                "method": limpar_texto(form.get("method", "GET")).upper(),
                "inputs": [
                    {
                        "name": limpar_texto(inp.get("name", "")),
                        "type": limpar_texto(inp.get("type", "text")).lower(),
                        "placeholder": limpar_texto(inp.get("placeholder", "")),
                    }
                    for inp in form.find_all("input")
                ],
                "buttons": [limpar_texto(btn.get_text(" ", strip=True)) for btn in form.find_all("button")],
            }
        )

    buttons = [limpar_texto(btn.get_text(" ", strip=True)) for btn in soup.find_all("button")]
    inputs = [
        {
            "name": limpar_texto(inp.get("name", "")),
            "type": limpar_texto(inp.get("type", "text")).lower(),
            "placeholder": limpar_texto(inp.get("placeholder", "")),
            "id": limpar_texto(inp.get("id", "")),
        }
        for inp in soup.find_all("input")
    ]

    return {
        "forms": forms,
        "buttons": buttons,
        "inputs": inputs,
    }


def imprimir_top_links(links, top_n=30):
    """Mostra os links mais relevantes primeiro."""
    ordenados = sorted(links, key=lambda x: x["score"], reverse=True)
    print("\n=== LINKS MAIS RELEVANTES ===")
    for item in ordenados[:top_n]:
        print(
            f"[{item['relevancia']}] score={item['score']} | texto='{item['text']}' | href='{item['href']}'"
        )


def agrupar_links_por_score(links):
    """Agrupa links por score para facilitar o mapeamento."""
    grupos = defaultdict(list)
    for link in links:
        grupos[link["score"]].append(link)
    return dict(grupos)


def main():
    """Executa a inspeção do HTML salvo."""
    if not os.path.exists(ARQUIVO_HTML):
        print(f"Erro: arquivo '{ARQUIVO_HTML}' não encontrado.")
        return

    try:
        with open(ARQUIVO_HTML, "r", encoding="utf-8", errors="replace") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")

        titulos = extrair_titulos(soup)
        links = extrair_links(soup)
        tabelas = extrair_tabelas(soup)
        estruturas_formulario = extrair_formularios(soup)

        print("=== TÍTULOS DA PÁGINA ===")
        print(f"Title: {titulos['title']}")
        print(f"H1: {titulos['h1']}")
        print(f"H2: {titulos['h2']}")
        print(f"H3: {titulos['h3']}")

        imprimir_top_links(links, top_n=40)

        print("\n=== RESUMO DE LINKS ===")
        print(f"Total de links: {len(links)}")
        print(f"Links com score > 0: {len([l for l in links if l['score'] > 0])}")
        print(f"Links com score 0: {len([l for l in links if l['score'] == 0])}")

        print("\n=== TABELAS ENCONTRADAS ===")
        if not tabelas:
            print("Nenhuma tabela encontrada.")
        for tabela in tabelas:
            print(f"\nTabela {tabela['index']}")
            print(f"Cabeçalhos: {tabela['headers']}")
            print(f"Amostra de linhas: {tabela['sample_rows']}")
            print(f"Total aproximado de linhas: {tabela['row_count']}")

        print("\n=== FORMULÁRIOS, BOTÕES E INPUTS ===")
        print(f"Formulários: {len(estruturas_formulario['forms'])}")
        print(f"Botões: {len(estruturas_formulario['buttons'])}")
        print(f"Inputs: {len(estruturas_formulario['inputs'])}")

        report = {
            "arquivo_html": ARQUIVO_HTML,
            "titulos": titulos,
            "links": links,
            "links_agrupados_por_score": agrupar_links_por_score(links),
            "tabelas": tabelas,
            "formularios": estruturas_formulario["forms"],
            "buttons": estruturas_formulario["buttons"],
            "inputs": estruturas_formulario["inputs"],
        }

        with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\nRelatório salvo em: {os.path.abspath(ARQUIVO_JSON)}")

    except Exception as e:
        print(f"Erro ao processar o HTML: {e}")


if __name__ == "__main__":
    main()
