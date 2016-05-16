import logging
import scrapy
from mercadolibre_scraping_spider.items import CategoriaItem

class MercadoLibreSpider(scrapy.Spider):
    name = "MercadoLibreSpider"
    allowed_domains = ["http://www.mercadolibre.com.ar/"]
    start_urls = [
        "http://www.mercadolibre.com.ar/"
    ]

    logger = logging.getLogger()

    #Parsing the MercadoLibre Site
    def parse(self, response):

        #First, we extract the categories from MercadoLibre
        div = response.css("#categories")
        categorias = div.css("a")
        self.logger.info('Getting the categories data...')

        result = self.saveCategorias(categorias)

        self.logger.info('...categories data saved.')

        return result
        #Then we go for a given category and extract its products.

    #Saving a given category and taking the due business action.
    def saveCategorias(self, categorias):

        self.logger.debug('Saving categories...')

        for categoria in categorias:
            cateItem = CategoriaItem()

            cateItem["linkCategoria"] = categoria.xpath(".//@href").extract()
            cateItem["nombre"] = categoria.xpath(".//text()").extract()

            self.logger.debug('Saving category:' + str(cateItem["nombre"]).encode("utf-8"))

            yield cateItem
