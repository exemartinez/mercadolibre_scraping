# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class MercadolibreScrapingPipeline(object):
    def process_item(self, item, spider):
        if (type(item) is CategoriaItem):
            print "Scraped pipelined item: " + item
        else:
            print "Unknown type"
        return item
