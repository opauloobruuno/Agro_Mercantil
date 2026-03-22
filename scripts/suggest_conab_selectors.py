import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


ARQUIVO_HTML = Path("conab_page.html")
DOMINIO_BASE = "https://portaldeinformacoes.conab.gov.br"


def limpar_texto(texto: str) -> str:
    if not texto:
        return ""
    return re.sub(r"\s+", " ", texto).strip()


def pontuar_selector(tag_name: str, attrs: dict, texto: str, parent_tag: str = "") -> int:
    score = 0
    texto_lower = (texto or "").lower()
    classes = " ".join(attrs.get("class", []) if isinstance(attrs.get("class"), list) else [attrs.get("class", "")]).lower()
    _id = (attrs.get("id") or "").lower()
    name = (attrs.get("name") or "").lower()
    href = (attrs.get("href") or "").lower()

    keywords = [
        "soja", "milho", "trigo", "algodão", "algodao", "café", "cafe",
        "arroz", "cana", "preço", "preco", "série", "serie",
        "histórico", "historico", "commodity", "indicador", "mercado"
    ]

    content = " ".join([tag_name, parent_tag, texto_lower, classes, _id, name, href])
    for kw in keywords:
        if kw in content:
            score += 2

    if tag_name in {"table", "a", "button", "select"}:
        score += 2
    if "table" in classes or "table" in _id:
        score += 2
    if "filter" in classes or "search" in classes or "query" in classes:
        score += 1

    return score


def ler_html() -> BeautifulSoup:
    if not ARQUIVO_HTML.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ARQUIVO_HTML.resolve()}")

    html = ARQUIVO_HTML.read_text(encoding="utf-8", errors="replace")
    return BeautifulSoup(html, "html.parser")


def coletar_titulos(soup: BeautifulSoup) -> dict:
    titles = {
        "title": limpar_texto(soup.title.get_text()) if soup.title else "",
        "h1": [limpar_texto(x.get_text(" ", strip=True)) for x in soup.find_all("h1")],
        "h2": [limpar_texto(x.get_text(" ", strip=True)) for x in soup.find_all("h2")],
        "h3": [limpar_texto(x.get_text(" ", strip=True)) for x in soup.find_all("h3")],
    }
    return titles


def coletar_links(soup: BeautifulSoup) -> list[dict]:
    links = []
    for a in soup.find_all("a", href=True):
        texto = limpar_texto(a.get_text(" ", strip=True))
        href = limpar_texto(a.get("href", ""))
        abs_url = urljoin(DOMINIO_BASE, href)
        score = pontuar_selector("a", a.attrs, texto)
        links.append(
            {
                "text": texto,
                "href": href,
                "absolute_url": abs_url,
                "score": score,
                "is_internal": urlparse(abs_url).netloc.endswith("conab.gov.br"),
                "likely_selector": "high" if score >= 5 else "medium" if score >= 2 else "low",
            }
        )
    return links


def coletar_tabelas(soup: BeautifulSoup) -> list[dict]:
    tables = []
    for idx, table in enumerate(soup.find_all("table"), start=1):
        headers = []
        if table.find("thead"):
            headers = [limpar_texto(th.get_text(" ", strip=True)) for th in table.find_all("th")]
        else:
            first_row = table.find("tr")
            if first_row:
                headers = [limpar_texto(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]

        rows = []
        for tr in table.find_all("tr")[1:5]:
            cells = [limpar_texto(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)

        tables.append(
            {
                "index": idx,
                "headers": headers,
                "sample_rows": rows,
                "row_count": len(table.find_all("tr")),
                "classes": table.get("class", []),
                "id": table.get("id", ""),
                "score": pontuar_selector("table", table.attrs, " ".join(headers)),
            }
        )
    return tables


def coletar_formularios(soup: BeautifulSoup) -> dict:
    forms = []
    for form in soup.find_all("form"):
        inputs = []
        selects = []
        buttons = []

        for inp in form.find_all("input"):
            inputs.append(
                {
                    "name": inp.get("name", ""),
                    "type": inp.get("type", "text"),
                    "id": inp.get("id", ""),
                    "placeholder": inp.get("placeholder", ""),
                    "score": pontuar_selector("input", inp.attrs, inp.get("placeholder", "") or inp.get("name", "")),
                }
            )

        for sel in form.find_all("select"):
            selects.append(
                {
                    "name": sel.get("name", ""),
                    "id": sel.get("id", ""),
                    "score": pontuar_selector("select", sel.attrs, sel.get("name", "")),
                }
            )

        for btn in form.find_all(["button", "input"]):
            txt = limpar_texto(btn.get_text(" ", strip=True)) if btn.name == "button" else limpar_texto(btn.get("value", ""))
            buttons.append(
                {
                    "text": txt,
                    "type": btn.get("type", ""),
                    "score": pontuar_selector(btn.name, btn.attrs, txt),
                }
            )

        forms.append(
            {
                "action": form.get("action", ""),
                "method": form.get("method", "get"),
                "inputs": inputs,
                "selects": selects,
                "buttons": buttons,
                "score": pontuar_selector("form", form.attrs, " ".join(i["name"] for i in inputs)),
            }
        )

    return {
        "forms": forms,
        "buttons_total": len(soup.find_all("button")),
        "inputs_total": len(soup.find_all("input")),
        "selects_total": len(soup.find_all("select")),
    }


def sugerir_selectores(links: list[dict], tables: list[dict], forms: dict) -> dict:
    top_links = sorted(links, key=lambda x: x["score"], reverse=True)[:20]
    top_tables = sorted(tables, key=lambda x: x["score"], reverse=True)[:10]
    top_forms = sorted(forms["forms"], key=lambda x: x["score"], reverse=True)[:5]

    return {
        "top_links": top_links,
        "top_tables": top_tables,
        "top_forms": top_forms,
    }


def main():
    soup = ler_html()

    titulos = coletar_titulos(soup)
    links = coletar_links(soup)
    tables = coletar_tabelas(soup)
    forms = coletar_formularios(soup)
    sugestoes = sugerir_selectores(links, tables, forms)

    print("\n=== TÍTULOS ===")
    print(f"Title: {titulos['title']}")
    print(f"H1: {titulos['h1']}")
    print(f"H2: {titulos['h2']}")
    print(f"H3: {titulos['h3']}")

    print("\n=== LINKS MAIS PROMISSORES PARA O SPIDER ===")
    for item in sugestoes["top_links"]:
        print(f"[{item['likely_selector']}] score={item['score']} | {item['text']} -> {item['href']}")

    print("\n=== TABELAS MAIS PROMISSORAS ===")
    for item in sugestoes["top_tables"]:
        print(f"Tabela {item['index']} | score={item['score']} | headers={item['headers']}")
        print(f"  amostra={item['sample_rows']}")

    print("\n=== FORMULÁRIOS / FILTROS ===")
    print(f"Formulários: {len(forms['forms'])}")
    print(f"Buttons: {forms['buttons_total']}")
    print(f"Inputs: {forms['inputs_total']}")
    print(f"Selects: {forms['selects_total']}")

    print("\n=== DICAS DE SELECTORES ===")
    if sugestoes["top_tables"]:
        print("Possível seletor base para tabelas: table, table thead th, table tbody tr")
    if sugestoes["top_links"]:
        print("Possível seletor base para links: a[href] com filtragem por palavras-chave")
    if forms["forms"]:
        print("Possível seletor base para filtros: form, select, input, button")

    # Pequena heurística extra: listar classes e ids mais frequentes
    classes = defaultdict(int)
    ids = defaultdict(int)
    for tag in soup.find_all(True):
        for c in tag.get("class", []):
            classes[c] += 1
        if tag.get("id"):
            ids[tag.get("id")] += 1

    top_classes = sorted(classes.items(), key=lambda x: x[1], reverse=True)[:15]
    top_ids = sorted(ids.items(), key=lambda x: x[1], reverse=True)[:15]

    print("\n=== CLASSES MAIS FREQUENTES ===")
    for c, n in top_classes:
        print(f"{c}: {n}")

    print("\n=== IDS MAIS FREQUENTES ===")
    for i, n in top_ids:
        print(f"{i}: {n}")


if __name__ == "__main__":
    main()
