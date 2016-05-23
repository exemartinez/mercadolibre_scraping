import logging
from pysqlite2 import dbapi2 as sqlite

class DAO (object):

    logger = logging.getLogger()

    def exec_new_single_sub_categoria(self, linkCategoria, nombre, ml_categorias_id_fk):
        '''
        Creates a new Categoria in the database.
        '''
        self.cursor.execute("insert into ml_sub_categorias (linkCategoria, nombre, ml_categorias_id_fk) values (?, ?, ?)", (linkCategoria, nombre, ml_categorias_id_fk,))
        self.connection.commit()

    def exec_new_single_categoria(self, linkCategoria, nombre):
        '''
        Creates a new Categoria in the database.
        '''
        self.cursor.execute("insert into ml_categorias (linkCategoria, nombre) values (?, ?)", (linkCategoria, nombre,))
        self.connection.commit()

    def exec_get_sub_categoria_exists_byLink(self, linkCategoria):
        '''
        Returns one, single sub categoria by its URL
        '''
        self.cursor.execute("select * from ml_sub_categorias where linkCategoria=?", (linkCategoria,))
        return self.cursor.fetchone()

    def exec_get_categoria_exists_byLink(self, linkCategoria):
        '''
        Returns one, single categoria by its URL
        '''
        self.logger.debug("Trying to assess if the new category exists in ml_categorias...")

        self.cursor.execute("select * from ml_categorias where linkCategoria=?", (linkCategoria,))

        result = self.cursor.fetchone()
        self.logger.debug("RESULT: " + str(result))

        return result

    def exec_get_all_categoria(self):
        '''
        Returns every categoria that is in place (full scan)
        '''
        self.logger.debug("Fetching...")
        self.cursor.execute("select id, linkCategoria, nombre from ml_categorias")
        return self.cursor.fetchall()

    def open_connection(self):
        '''
        Sets the due connections to the data stores.
        '''

        self.logger.debug("Initiating database")
        # Initializes the connection to SQLite (and creates the due tables)
        self.connection = sqlite.connect('./ml_scrapedata.db')
        self.cursor = self.connection.cursor()

        #Creates the database TABLES, if there is NONE
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ml_categorias ' \
                    '(id INTEGER PRIMARY KEY, linkCategoria VARCHAR(4000), nombre VARCHAR(250))')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ml_sub_categorias ' \
                    '(id INTEGER PRIMARY KEY, ml_categorias_id_fk INTEGER,linkCategoria VARCHAR(4000), nombre VARCHAR(250))')

        self.logger.debug("Database, READY to throw operations at her.")

    def close_connection(self):
        '''
        Closes the database connection to avoid issues related to the connectivity.
        '''
        self.logger.debug("Database offline.")
        self.cursor.close()
