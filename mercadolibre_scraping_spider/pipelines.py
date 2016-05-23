# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from mercadolibre_scraping_spider.items import CategoriaItem, SubCategoriaItem
from mercadolibre_scraping_spider.dao import DAO
from scrapy.exceptions import DropItem
from pysqlite2 import dbapi2 as sqlite

class MercadolibreScrapingPipeline(object):
    """
    This is a pipeline class, and the idea is that it persist the data in different boxes: SQL, NOSQL, JSON or a simple FILE.
    Edit or subclass 'process_item' in order to do so.
    """
    logger = logging.getLogger()
    daoinst = None

    def __init__(self):
        self.daoinst = DAO()

    def process_item(self, item, spider):
        '''
        Handles the storage of all the scraped data for further explotation.
        '''

        self.logger.debug("Storing the data...")

        if (type(item) is CategoriaItem):
            #Shooting the first select for not redeeming data that is already there.
            # Initializes the connection to SQLite (and creates the due tables)
            self.daoinst.open_connection()

            #preparing the parameters...
            nombre_cat = str(item['nombre']).encode("utf-8")
            link_cat = str(item['linkCategoria']).encode("utf-8")

            result = self.daoinst.exec_get_categoria_exists_byLink(link_cat)

            if result:
                self.logger.debug("Categoria already in database: " + nombre_cat)
            else:
                self.daoinst.exec_new_single_categoria(link_cat, nombre_cat)
                self.logger.debug("Succesfully stored pipelined Categoria: " + nombre_cat)

            self.daoinst.close_connection()

        if (type(item) is SubCategoriaItem):
            #Starting to store the SUB categories data from MercadoLibre.
            self.daoinst.open_connection()

            result = self.daoinst.exec_get_sub_categoria_exists_byLink(item['linkCategoria'][0])

            if result:
                self.logger.debug("SubCategoria already in database: " + str(item['nombre']).encode("utf-8"))
            else:
                self.daoinst.exec_new_single_sub_categoria(item['linkCategoria'][0], item['nombre'][0], item['ml_categorias_id_fk'][0] )
                self.logger.debug("Succesfully stored pipelined SubCategoria: " + str(item['nombre']).encode("utf-8"))

            self.daoinst.close_connection()
        else:
            raise DropItem("Not recognizable item.")
            self.logger.debug("Unknown type")

        return item

    def open_spider(self, spider):
        '''
        Sets the due connections to the data stores.
        '''
        self.logger.debug("MercadoLibre PIPELINES - Online")

    def close_spider(self, spider):
        self.logger.debug("MercadoLibre PIPELINES - Offline")
