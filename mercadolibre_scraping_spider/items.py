# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

#Gets all the data related to the products categories available in MercadoLibre
class CategoriaItem(Item):

    nombre = Field()
    linkCategoria = Field()
