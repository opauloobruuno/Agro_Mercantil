import scrapy


class ScraperItem(scrapy.Item):
    pass


class AgriculturalCommodityItem(scrapy.Item):
    """Item para armazenar dados de precos de commodities agricolas da Conab."""

    commodity = scrapy.Field()
    region = scrapy.Field()
    market_unit = scrapy.Field()
    date = scrapy.Field()
    price = scrapy.Field()
    currency = scrapy.Field()
    source_name = scrapy.Field()
    source_url = scrapy.Field()
    captured_at = scrapy.Field()
    notes = scrapy.Field()
