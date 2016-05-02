import scrapy

class MercadoLibreSpider(scrapy.Spider):
    name = "MercadoLibreSpider"
    allowed_domains = ["http://www.mercadolibre.com.ar/"]
    start_urls = [
        "http://www.mercadolibre.com.ar/"
    ]

    #Parsing the MercadoLibre Site
    def parse(self, response):

        #First, we extract the categories from MercadoLibre
        div = response.css("#categories")
        categorias = div.css("a")

        self.saveCategorias(categorias)

        #Then we go for a given category and extract its products.

    #Saving a given category and taking the due business action.
    def saveCategorias(self, categorias):

        for categoria in categorias:
            cateItem = CategoriaItem()

            cateItem["linkCategoria"] = categoria.xpath(".//@href").extract()
            cateItem["nombre"] = categoria.xpath(".//text()").extract()

            print "From the site: " + categoria.xpath(".//text()").extract()

            yield cateItem
