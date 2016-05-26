import logging
import locale
import scrapy
import pdb
from mercadolibre_scraping_spider.items import CategoriaItem, SubCategoriaItem
from mercadolibre_scraping_spider.dao import DAO

class ML_Categorias_Spider(scrapy.Spider):
    '''
    This class scraps Mercadolibre.com.ar for products categories.
    '''

    name = "ML_Categorias_Spider"
    allowed_domains = ["http://www.mercadolibre.com.ar/",
                       "http://home.mercadolibre.com.ar/"]
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

    '''
    Saving a given category and taking the due business action.
    '''
    def saveCategorias(self, categorias):

        self.logger.debug('Saving categories...')

        for categoria in categorias:
            cateItem = CategoriaItem()

            cateItem["linkCategoria"] = str(categoria.xpath(".//@href").extract()).encode('utf-8')
            cateItem["nombre"] = str(categoria.xpath(".//text()").extract()).encode('utf-8')

            self.logger.debug('Saving category: ' + cateItem["nombre"])

            yield cateItem

class ML_SubCategorias_Spider(scrapy.Spider):
    '''
    This class scraps Mercadolibre.com.ar for products SUB categories.
    '''
    CONST_ML_SUBCAT_URL = 1
    name = "ML_SubCategorias_Spider"
    allowed_domains = ["http://www.mercadolibre.com.ar/",
                       "http://home.mercadolibre.com.ar/"]
    start_urls = [
        "http://www.mercadolibre.com.ar/"
    ]

    logger = logging.getLogger()

    #Parsing the MercadoLibre Site
    def parse(self, response):
        #pdb.set_trace()
        #Gets the categories to the pages (urls) to get into...
        daoinstance = DAO()
        next_url=None

        #extracts the data from the database.
        daoinstance.open_connection()
        results = daoinstance.exec_get_all_categoria()
        daoinstance.close_connection()

        self.logger.debug('Categories returned: ' + str(len(results)))

        #iterates the subcategories...
        for item in results:
            next_url = self.fix_unicode(item[self.CONST_ML_SUBCAT_URL])
            self.logger.debug('Processing the category link: %s', next_url)

            #starts redirecting to the die HTTP-str-subcategories

            if (next_url.startswith("http:")): #avoids any wrong scraped url string

                request = scrapy.Request(next_url, callback=self.parse_subCategorias, dont_filter=True)
                request.meta["id_cat"] = item[0]
                self.logger.debug('Redirecting to the next URL for processing')

                yield request

        self.logger.debug('Parsing subcategories - DONE')


    def parse_subCategorias(self, response):
        '''
        We take the subcategories
        '''
        self.logger.debug("Visited %s", response.url)

        id_cat = response.meta["id_cat"]
        lista = response.xpath("//body/div[@class='container']/div[@class='nav']")

        for subcater in lista.xpath("./div/ul/li/a"):

            subcat = SubCategoriaItem()

            subcat["nombre"] = str(subcater.xpath("./text()").extract()).encode('utf-8')
            subcat["linkCategoria"] = str(subcater.xpath("./@href").extract()).encode('utf-8')
            subcat["ml_categorias_id_fk"] = id_cat

            self.logger.debug('Subcategorie url: ' + subcat["linkCategoria"])
            self.logger.debug('Subcategorie nombre: ' + subcat["nombre"])

            yield subcat

    def fix_unicode(self, uString):
        '''
        This resolves the error of URL stored with "[u'" and "']'" at the end of the string.
        '''
        #TODO: I need to replace this, for the proper way to transform UNICODE to another character encoding; one that works with the current database.
        
        language, output_encoding = locale.getdefaultlocale()
        return uString.encode(output_encoding)[3:-2]
