import scrapy


class MinhaSpiderSpider(scrapy.Spider):
    name = "minha_spider"
    allowed_domains = ["exemplo.com"]
    start_urls = ["https://exemplo.com"]

    def parse(self, response):
        pass
