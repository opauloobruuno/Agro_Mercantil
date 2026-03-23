# Agro Mercantil

Projeto para coletar e preparar dados de precos agropecuarios (serie historica) a partir do portal da Conab, com foco em:

- coletar HTML e tabelas brutas para auditoria (`data/raw/`)
- extrair itens estruturados (commodities, precos e datas) via Scrapy (`src/scraper/`)
- documentar sugestoes de seletores para evoluir o scraper (`docs/`)

## Estrutura do repositorio

- `data/raw/` -> arquivos originais coletados (ex.: `conab_page.html`, `conab_raw.csv/json`)
- `data/processed/` -> dados intermediarios tratados (a definir)
- `data/curated/` -> dados prontos para analise/consulta (a definir)
- `sql/` -> scripts SQL (ex.: `sql/schema.sql` para criar o banco e `sql/queries_analise.sql` para queries/BI)
- `src/` -> codigo Python do scraper (Scrapy)
  - `src/scraper/` -> projeto Scrapy (`scrapy.cfg` + spiders)
  - `src/scraper/agro_scraping/` -> itens, pipelines e spiders
- `scripts/` -> utilitarios (baixar/inspecionar/sugerir seletores)
- `docs/` -> documentacao dos seletores e heuristicas
- `app/` -> aplicacao (placeholder; hoje esta vazio)
- `tests/` -> testes unitarios (placeholder)

## Requisitos

O arquivo `requirements.txt` cobre dependencias usadas nos scripts utilitarios:

- `requests`
- `beautifulsoup4`
- `pandas`

Para rodar o scraper Scrapy, e necessario instalar o Scrapy (nao esta listado em `requirements.txt`):

```bash
pip install scrapy
```

## Como baixar e inspecionar a pagina (scripts)

### 1) Baixar HTML e exportar tabelas brutas

Usa `requests` + `BeautifulSoup` para salvar:

- `data/raw/conab_page.html`
- `data/raw/conab_raw.csv`
- `data/raw/conab_raw.json`

Comando:

```bash
python scripts/download_conab_raw_tables.py
```

### 2) (Opcional) Baixar apenas o HTML

Salva `conab_page.html` no diretorio de execucao:

```bash
python scripts/download_conab_page.py
```

### 3) Inspecionar o HTML salvo

Lera `conab_page.html` e gera `conab_page_inspection.json`:

```bash
python scripts/inspect_conab_page.py
```

### 4) Sugerir seletores com heuristicas

Tambem assume que existe `conab_page.html` no diretorio atual:

```bash
python scripts/suggest_conab_selectors.py
```

## Spider Scrapy: `conab_prices`

O spider `conab_prices` esta em `src/scraper/agro_scraping/spiders/conab_prices.py`.
Ele faz:

1. Comeca na pagina de "precosa agropecuarios - serie historica"
2. Descobre links de commodities filtrando `href` por palavras-chave (soja, milho, trigo, etc.)
3. Para cada commodity, percorre linhas de tabelas e tenta extrair:
   - `date` (com parse por multiplos formatos)
   - `price` (parse aceitando formato brasileiro)
4. Segue pagina "next" quando encontrada.

### Execucao

Rode a partir da pasta que contem `scrapy.cfg` (hoje: `src/scraper/`):

```bash
cd src/scraper
scrapy crawl conab_prices -O data/curated/conab_prices.json
```

O spider aceita parametro opcional `start_url`:

```bash
cd src/scraper
scrapy crawl conab_prices -a start_url="https://..." -O data/curated/conab_prices.json
```

## Campos do item extraido

O pipeline/padroes do projeto definem o item `AgriculturalCommodityItem` com estes campos:

- `commodity`
- `region`
- `market_unit`
- `date` (normalizado para ISO, quando possivel)
- `price` (float)
- `currency` (por padrao `BRL`)
- `source_name` (por padrao `Conab`)
- `source_url` (URL de origem)
- `captured_at` (UTC ISO)
- `notes` (string com contexto quando date ou price nao sao encontrados)

## Pipeline de limpeza/validacao

O projeto usa o pipeline `AgriculturalCommodityPipeline` (`src/scraper/agro_scraping/pipelines.py`):

- descarta itens sem `commodity`, `date` ou `price`
- normaliza campos de texto (trim)
- parseia `date` e `price` (com heuristicas)
- garante defaults:
  - `currency` = `BRL` (se ausente)
  - `notes` = `""` (se ausente)
- adiciona `captured_at` (se ausente)

## Documentacao de seletores

Veja `docs/conab_spider_selectors.md` para:

- sugestoes de CSS/XPath para titulos, links, tabelas e formularios
- exemplo de uso de `scrapy shell` para validar seletores

## Status atual

- Scraper Scrapy e scripts de apoio estao implementados.
- `app/` e `tests/` estao como placeholders (sem codigo adicional ate o momento).
