# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from mercadolibre_scraping_spider.items import CategoriaItem
from scrapy.exceptions import DropItem

class MercadolibreScrapingPipeline(object):
""" This is a pipeline class, and the idea is that it persist the data in different boxes: SQL, NOSQL, JSON or a simple FILE.
Edit or subclass 'process_item' in order to do so.
"""
    logger = logging.getLogger()


    def process_item(self, item, spider):

        self.logger.debug("Storing the data...")

        if (type(item) is CategoriaItem):
            self.logger.debug("Scraped pipelined item: " + str(item).encode("utf-8"))
        else:
            raise DropItem("Not recognizable item.")
            self.logger.debug("Unknown type")

        return item

    def open_spider(self, spider):
        self.logger.debug("MercadoLibre PIPELINES - Online")

    def close_spider(self, spider):
        self.logger.debug("MercadoLibre PIPELINES - Offline")
