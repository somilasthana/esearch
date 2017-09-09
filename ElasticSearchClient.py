from PConfig import PConfig
from PConstant import PConstant

import elasticsearch
import requests
import logging

class ElasticSearchClient(object):

    def __init__(self):
        self.config = PConfig()
        self.ip = self.config[PConstant.ELASTICSEARCHIP_CONFIG.value].split(":")[0]
        self.port = 9200
        self.es = elasticsearch.Elasticsearch([{'host': self.ip, 'port': self.port}])
        self._flogger()

    def get_indexes(self):
        return self.es.indices.get_alias()

    def create_index(self, indexName='default', request_body = {}):

        if self.es.indices.exists(indexName):
            self._logger.info("deleting '%s' index...", indexName)
            res = self.es.indices.delete(index = indexName)
            self._logger.info(" response: '%s'", res)

        self._logger.info("creating '%s' index...", indexName)
        res = self.es.indices.create(index = indexName, body = request_body)
        self._logger.info("response: '%s' ", res)        

    def delete_index(self, indexName='default'):

        if self.indices.exists(indexName):
            self._logger.info("delete '%s' index...", indexName)
            res = self.es.indices.delete(index = indexName)
            self._logger.info("response: '%s' ",res)
        else:
            self._logger.info(" index '%s' missing !! ", indexName)
 
    def add_document(self, indexName, typeName, row, collist):

        eslocal = elasticsearch.Elasticsearch([{'host': self.ip, 'port': self.port}])
        data = {}
        self._logger.info("inserting in '%s' and '%s' ", indexName, typeName)
        for col in collist:
            data[col] = row[col]
        res = eslocal.index(index=indexName, body=data, doc_type=typeName)
        self._logger.info("done inserting in '%s' and '%s' ", indexName, typeName)
            
    def add_bulk_document(self, indexName, bulk_data):

        eslocal = elasticsearch.Elasticsearch([{'host': self.ip, 'port': self.port}])
        self._logger.info("inserting bulk in '%s'", indexName)
        eslocal.bulk(index = indexName, body = bulk_data, refresh = True)
        self._logger.info("done bulk inserting in '%s'", indexName)

    def search_result(self, indexName, typeName, query):
        self._logger.info("searching and extracting results '%s'", indexName)
        res = self.es.search(index=indexName, doc_type=typeName, body=query)
        return res 

    def scan_result_sync(self, indexName, typeName, query):
        self._logger.info("initializing scroll for '%s'", indexName)
        #query={"query": {"match" : {"publisher": "Soar Printing"}}}
        page = self.es.search(index = indexName, doc_type = typeName, scroll = '2m',  size = 1000, body = query )
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        pagelist = []
        # Start scrolling
        while (scroll_size > 0):
            self._logger.info("scrolling '%s'", indexName)
            page = self.es.scroll(scroll_id = sid, scroll = '2m')
            pagelist.append(page['hits']['hits'])
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
        return pagelist

    def scan_result_cb(self, indexName, typeName, query, cb):
        self._logger.info("initializing scroll for '%s'", indexName)
        page = self.es.search(index = indexName, doc_type = typeName, scroll = '2m', search_type = 'scan', size = 1000, body = query)
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        # Start scrolling
        while (scroll_size > 0):
            self._logger.info("scrolling '%s'", indexName)
            page = self.es.scroll(scroll_id = sid, scroll = '2m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            cb(page)
      



    def _flogger(self):

        self._logger = logging.getLogger('ElasticSearchClient')
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

