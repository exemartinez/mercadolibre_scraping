# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from mercadolibre_scraping_spider.items import CategoriaItem
from scrapy.exceptions import DropItem
from pysqlite2 import dbapi2 as sqlite

class MercadolibreScrapingPipeline(object):
    """
    This is a pipeline class, and the idea is that it persist the data in different boxes: SQL, NOSQL, JSON or a simple FILE.
    Edit or subclass 'process_item' in order to do so.
    """
    logger = logging.getLogger()

    def process_item(self, item, spider):
        '''
        Handles the storage of all the scraped data for further explotation.
        '''

        self.logger.debug("Storing the data...")

        if (type(item) is CategoriaItem):
            #Shooting the first select for not redeeming data that is already there.
            self.cursor.execute("select * from ml_categorias where linkCategoria=?", item['linkCategoria'])
            result = self.cursor.fetchone()

            if result:
                self.logger.debug("Item already in database: " + str(item['nombre']).encode("utf-8"))
            else:
                self.cursor.execute("insert into ml_categorias (linkCategoria, nombre) values (?, ?)", (item['linkCategoria'][0], item['nombre'][0]))
                self.connection.commit()
                self.logger.debug("Succesfully stored pipelined item: " + str(item['nombre']).encode("utf-8"))

        else:
            raise DropItem("Not recognizable item.")
            self.logger.debug("Unknown type")

        return item

    def open_spider(self, spider):
        '''
        Sets the due connections to the data stores.
        '''
        self.logger.debug("MercadoLibre PIPELINES - Online")

        # Initializes the connection to SQLite (and creates the due tables)
        self.connection = sqlite.connect('./ml_scrapedata.db')
        self.cursor = self.connection.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ml_categorias ' \
                    '(id INTEGER PRIMARY KEY, linkCategoria VARCHAR(4000), nombre VARCHAR(250))')

    def close_spider(self, spider):
        self.logger.debug("MercadoLibre PIPELINES - Offline")
        self.cursor.close()
