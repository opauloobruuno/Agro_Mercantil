# Configuracoes para o projeto agro_scraping
# Este arquivo define as configuracoes do spider conab_prices

BOT_NAME = "agro_scraping"

SPIDER_MODULES = ["agro_scraping.spiders"]
NEWSPIDER_MODULE = "agro_scraping.spiders"

# Obedecer as regras do robots.txt
ROBOTSTXT_OBEY = True

# Atraso entre requisicoes para ser mais educado com o site
DOWNLOAD_DELAY = 1

# AutoThrottle para ajustar a taxa de acesso conforme a resposta do site
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# Tentativas automaticas em caso de falha
RETRY_ENABLED = True

# Cookies desabilitados para reduzir rastreamento e variabilidade
COOKIES_ENABLED = False

# Pipeline de processamento dos itens
ITEM_PIPELINES = {
    "agro_scraping.pipelines.AgriculturalCommodityPipeline": 300,
}

# Log em nivel informativo
LOG_LEVEL = "INFO"

# User-Agent educado para o scraper
USER_AGENT = "agro_scraping (+http://www.yourdomain.com)"
