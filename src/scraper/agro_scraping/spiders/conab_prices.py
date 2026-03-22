import re
from datetime import datetime

import scrapy

from agro_scraping.items import AgriculturalCommodityItem


class ConabPricesSpider(scrapy.Spider):
    """
    Spider para extrair precos agricolas historicos do portal Conab.
    """

    name = "conab_prices"

    def __init__(self, start_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [start_url] if start_url else [
            "https://portaldeinformacoes.conab.gov.br/precos-agropecuarios-serie-historica.html"
        ]
        self.source_name = "Conab"
        self.currency = "BRL"

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_homepage)

    def parse_homepage(self, response):
        commodity_links = self._discover_commodity_links(response)

        for link in commodity_links:
            yield response.follow(link, callback=self.parse_commodity)

    def parse_commodity(self, response):
        commodity_name = self._guess_commodity_name(response)

        table_rows = response.css("table tr")
        if not table_rows:
            table_rows = response.css("tbody tr")

        for row in table_rows:
            item = self._extract_row_item(row, response, commodity_name)
            if item:
                yield item

        next_page = response.css('a[rel="next"]::attr(href)').get()
        if not next_page:
            next_page = response.css("a.next::attr(href)").get()
        if not next_page:
            next_page = response.xpath(
                '//a[contains(translate(normalize-space(.), "NEXTPROXIMA", "nextproxima"), "prox")]/@href'
            ).get()

        if next_page:
            yield response.follow(next_page, callback=self.parse_commodity)

    def _discover_commodity_links(self, response):
        links = response.css("a::attr(href)").getall()
        filtered = []

        for link in links:
            if not link:
                continue
            href = link.lower()

            if any(
                token in href
                for token in [
                    "soja",
                    "milho",
                    "trigo",
                    "algodao",
                    "cafe",
                    "arroz",
                    "cana",
                    "commod",
                    "preco",
                    "indicador",
                    "serie",
                ]
            ):
                filtered.append(link)

        if filtered:
            return list(dict.fromkeys(filtered))

        fallback = response.css(
            "nav a::attr(href), .menu a::attr(href), .list a::attr(href)"
        ).getall()
        return list(dict.fromkeys([l for l in fallback if l]))

    def _guess_commodity_name(self, response):
        title = response.css("title::text").get() or ""
        h1 = response.css("h1::text").get() or ""
        h2 = response.css("h2::text").get() or ""

        text = " ".join([title, h1, h2]).strip()
        return self.clean_text(text)

    def _extract_row_item(self, row, response, commodity_name):
        cells = [self.clean_text(t) for t in row.css("th::text, td::text").getall()]
        cells = [c for c in cells if c]

        if len(cells) < 2:
            return None

        raw_text = " | ".join(cells)

        date = None
        price = None
        region = ""
        market_unit = ""
        notes = ""

        for cell in cells:
            if date is None:
                parsed_date = self.parse_date(cell)
                if parsed_date:
                    date = parsed_date
                    continue

            if price is None:
                parsed_price = self.parse_price(cell)
                if parsed_price is not None:
                    price = parsed_price
                    continue

        if len(cells) >= 2:
            region = cells[1]
        if len(cells) >= 3:
            market_unit = cells[2]

        if date is None or price is None:
            notes = raw_text

        return AgriculturalCommodityItem(
            commodity=commodity_name,
            region=region,
            market_unit=market_unit,
            date=date,
            price=price,
            currency=self.currency,
            source_name=self.source_name,
            source_url=response.url,
            captured_at=datetime.utcnow().isoformat(),
            notes=notes,
        )

    def clean_text(self, text):
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    def parse_date(self, date_str):
        if not date_str:
            return None

        value = self.clean_text(date_str)

        formats = [
            "%d/%m/%Y",
            "%d/%m/%y",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%m/%Y",
            "%Y/%m/%d",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
                if fmt == "%m/%Y":
                    return dt.strftime("%Y-%m-01")
                return dt.date().isoformat()
            except ValueError:
                continue

        return None

    def parse_price(self, price_str):
        if not price_str:
            return None

        value = self.clean_text(price_str)
        value = value.replace("R$", "").replace("BRL", "").strip()

        if not value:
            return None

        value = value.replace(".", "").replace(",", ".")

        match = re.search(r"-?\d+(\.\d+)?", value)
        if not match:
            return None

        try:
            return float(match.group(0))
        except ValueError:
            return None
