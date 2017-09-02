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
 
    def _flogger(self):

        self._logger = logging.getLogger('ElasticSearchClient')
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

