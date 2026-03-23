# Sugestões de seletores para o spider da Conab

## 1) Títulos da página

### CSS

- `title`
- `h1`
- `h2`
- `h3`

### XPath

- `//title/text()`
- `//h1/text()`
- `//h2/text()`
- `//h3/text()`

---

## 2) Links de commodities / preços / histórico

### CSS

- `a[href]`
- `nav a[href]`
- `.menu a[href]`
- `.list a[href]`

### XPath

- `//a[@href]`
- `//nav//a[@href]`
- `//a[contains(translate(@href, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'soja')]`
- `//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚÃÕÇ', 'abcdefghijklmnopqrstuvwxyzáéíóúãõç'), 'preço')]`

### Filtro recomendado no spider

Priorizar links que contenham palavras como:

- soja
- milho
- trigo
- algodão
- café
- arroz
- cana
- preço
- série
- histórico
- commodity
- indicador

---

## 3) Tabelas de dados

### CSS

- `table`
- `table thead th`
- `table tbody tr`
- `table tr`

### XPath

- `//table`
- `//table//th`
- `//table//tr`
- `//table//tbody//tr`

### Estratégia sugerida

- Ler primeiro `thead`
- Se não houver `thead`, usar a primeira linha da tabela como cabeçalho
- Extrair as 3 a 5 primeiras linhas para validar o padrão

---

## 4) Filtros, formulários e controles

### CSS

- `form`
- `form input`
- `form select`
- `form button`
- `input[type="text"]`
- `input[type="search"]`
- `select[name]`

### XPath

- `//form`
- `//form//input`
- `//form//select`
- `//form//button`
- `//input[@type='text' or @type='search']`
- `//select[@name]`

### O que observar

- campos de produto
- campos de período/data
- filtros por localidade
- botões de pesquisa ou atualização

---

## 5) Como validar no Scrapy Shell

Abra a página no shell e teste:

```bash
scrapy shell "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"
```

Dentro do shell, experimente por exemplo:

```python
response.css("title::text").get()
response.css("a[href]::attr(href)").getall()[:20]
response.css("table tr").getall()[:3]
```

Para usar o projeto `agro_scraping`, execute a partir da pasta que contém `scrapy.cfg` (por exemplo `src/scraper/`):

```bash
cd src/scraper
scrapy shell "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"
```
