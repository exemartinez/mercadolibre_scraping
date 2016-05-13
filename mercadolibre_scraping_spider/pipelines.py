# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from mercadolibre_scraping_spider.items import CategoriaItem

class MercadolibreScrapingPipeline(object):

    logger = logging.getLogger()

    def process_item(self, item, spider):

        self.logger.debug("Executing the pipelines...")

        if (type(item) is CategoriaItem):
            self.logger.debug("Scraped pipelined item: " + item)
        else:
            self.logger.debug("Unknown type")
        return item
