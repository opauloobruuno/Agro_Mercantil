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
- `scripts/` -> utilitarios e modulos de apoio (`download_*.py`, `inspect_*`, `suggest_*`, `scraping.py`, `etl.py`, `analysis.py`, `eda_pandas.py`)
- `docs/` -> documentacao dos seletores e heuristicas
- `app/` -> aplicacao (`app/dashboard.py` para o dashboard Streamlit)
- `tests/` -> testes automatizados (`test_*.py`)

## Requisitos

O arquivo `requirements.txt` cobre dependencias usadas nos scripts utilitarios:

- `requests`
- `beautifulsoup4`
- `pandas`
- `scrapy`
- `psycopg2-binary`
- `streamlit`
- `plotly`
- `matplotlib`
- `pytest`

Para instalar tudo:

```bash
pip install -r requirements.txt
```

## Como baixar e inspecionar a pagina (scripts)

### 1) Baixar HTML e exportar tabelas brutas

Usa `requests` + `BeautifulSoup` para salvar:

- `data/raw/conab_page.html`
- `data/raw/conab_raw.csv`
- `data/raw/conab_raw.json`

O script cria `data/raw/` se ela nao existir.

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

1. Comeca na pagina de "precos agropecuarios - serie historica"
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

O `-O` exporta o feed para o arquivo informado; certifique-se de que `data/curated/` existe (o Scrapy nao cria diretorios intermediarios).

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

## ETL para PostgreSQL (opcional)

O script `scripts/etl_load.py.py` carrega os dados do CSV bruto para o PostgreSQL, usando o schema definido em `sql/schema.sql`.

Pontos importantes:

- O script procura `raw/conab_raw.csv` relativo ao diretorio de execucao.
- O destino das tabelas e o schema `agromercado` (conforme `sql/schema.sql`).

### Preparar o banco

1. Crie um banco PostgreSQL (o script usa `agromercado_db` como exemplo em `DB_CONFIG`).
2. Ajuste `DB_CONFIG` em `scripts/etl_load.py.py` (host/database/user/password).
3. Execute `sql/schema.sql` para criar o schema `agromercado` e as tabelas.

### Rodar o ETL

Para reaproveitar o output do `scripts/download_conab_raw_tables.py` (que grava em `data/raw/conab_raw.csv`), rode o ETL a partir do diretorio `data/`:

```bash
cd data
python ../scripts/etl_load.py.py
```

## SQL e analytics

- `sql/schema.sql` cria o schema `agromercado` e tabelas (`commodities`, `regioes`, `cargas_dados`, `precos`), alem de inserir dados de exemplo ficticios.
- `sql/queries_analise.sql` contem consultas de analise e tambem cria uma `VIEW materializada` chamada `agromercado.vw_dashboard_precos`.

## Dashboard interativo (Streamlit)

O dashboard em Streamlit esta em `app/dashboard.py` e consulta o PostgreSQL para exibir:

- Analise A: preco medio mensal por commodity com variacao mes a mes
- Analise B: top 5 commodities por volume no periodo selecionado
- Analise C: registros anomalos (negativos, fora de faixa e inconsistentes)
- Filtros por produto, regiao e periodo

### Configurar conexao com o banco (variaveis de ambiente)

O dashboard le:

- `DB_HOST` (default: `localhost`)
- `DB_DATABASE` (default: `agromercado_db`)
- `DB_USER` (default: `postgres`)
- `DB_PASSWORD` (default: `sua_senha`)

### Executar

Com as dependencias instaladas e o banco populado:

```bash
streamlit run app/dashboard.py
```

## Analise exploratoria (Pandas/Matplotlib)

Script:

```bash
python scripts/eda_pandas.py
```

Saidas geradas em `data/curated/eda/`:

- `estatisticas_descritivas.csv`
- `outliers_iqr.csv`
- `boxplot_precos_por_commodity.png`
- `histograma_precos.png`
- `scatter_data_preco.png`

## Documentacao final da avaliacao

- `docs/avaliacao_final.md` consolida:
  - aderencia aos itens da prova,
  - justificativas de modelagem e indices,
  - proposta de estrutura em S3,
  - insights, aplicacoes e limitacoes,
  - checklist de evidencias/prints (`docs/evidencias/`).

## Rodar testes

```bash
pytest -v
```

## Status atual

- Scraper Scrapy e scripts de apoio estao implementados.
- `scripts/etl_load.py.py` (ETL) e os scripts SQL (`sql/schema.sql` e `sql/queries_analise.sql`) estao presentes.
- Dashboard Streamlit esta disponivel em `app/dashboard.py`.
- `tests/` contem os testes automatizados do projeto.
