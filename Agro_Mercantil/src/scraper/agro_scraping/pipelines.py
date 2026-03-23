import logging
from datetime import datetime

from scrapy.exceptions import DropItem


class AgriculturalCommodityPipeline:
    """Pipeline para processar itens de commodities agricolas da Conab."""

    def __init__(self):
        """Inicializa os contadores de itens aceitos e descartados."""
        self.accepted_count = 0
        self.dropped_count = 0

    def process_item(self, item, spider):
        """
        Processa, valida e limpa o item do spider.

        - Descarta itens sem commodity, date ou price.
        - Normaliza campos de texto.
        - Normaliza datas para formato ISO quando possivel.
        - Converte price para float.
        - Define currency como 'BRL' se ausente.
        - Preenche notes com string vazia se ausente.
        """
        if not item.get("commodity") or not item.get("date") or not item.get("price"):
            self.dropped_count += 1
            logging.info(
                "Item descartado: faltam campos obrigatorios (commodity, date ou price). "
                f"Total descartados: {self.dropped_count}"
            )
            raise DropItem("Campos obrigatorios ausentes")

        for field in ["commodity", "currency", "notes", "region", "market_unit", "source_name", "source_url"]:
            if field in item and item[field]:
                item[field] = str(item[field]).strip()

        if "date" in item and item["date"]:
            date_str = str(item["date"]).strip()
            parsed_date = self._parse_date(date_str)
            if not parsed_date:
                self.dropped_count += 1
                logging.info(f"Data invalida: {date_str}. Item descartado.")
                raise DropItem("Formato de data invalido")
            item["date"] = parsed_date

        if "price" in item:
            parsed_price = self._parse_price(item["price"])
            if parsed_price is None:
                self.dropped_count += 1
                logging.info(f"Preco invalido: {item['price']}. Item descartado.")
                raise DropItem("Preco nao e um numero valido")
            item["price"] = parsed_price

        if not item.get("currency"):
            item["currency"] = "BRL"

        if not item.get("notes"):
            item["notes"] = ""

        if not item.get("captured_at"):
            item["captured_at"] = datetime.utcnow().isoformat()

        self.accepted_count += 1
        logging.info(f"Item aceito. Total aceitos: {self.accepted_count}")
        return item

    def close_spider(self, spider):
        """Registra o resumo final ao encerrar o spider."""
        logging.info(
            "Pipeline finalizado. Itens aceitos: %s, descartados: %s",
            self.accepted_count,
            self.dropped_count,
        )

    def _parse_date(self, value):
        """Converte datas comuns para ISO (YYYY-MM-DD) quando possivel."""
        if not value:
            return None

        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%m/%Y",
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

    def _parse_price(self, value):
        """Converte preco para float aceitando formato brasileiro."""
        if value is None:
            return None

        text = str(value).strip().replace("R$", "").replace("BRL", "").strip()
        if not text:
            return None

        text = text.replace(".", "").replace(",", ".")

        try:
            return float(text)
        except ValueError:
            return None
