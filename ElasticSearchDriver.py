import sys
import PConstant
import PConfig
import ElasticSearchClient
import ESDatabaseMetaStore
import logging
import json

class ElasticSearchDriver(object):

    def __init__(self, idname, indexname, typename):
        self.es = ElasticSearchClient.ElasticSearchClient()
        self.esdb = ESDatabaseMetaStore.ESDatabaseMetaStore()
        self.indexname = indexname
        self.typename = typename
        self.idfieldname = idname
        self._flogger()

    def createtable(self,  schemanamecount):
        request_body = self.esdb.get_request_body(schemanamecount)
        self._logger.info("Create Table ' %s '", self.indexname)
        self.es.create_index(self.indexname, request_body) 

    def pushbulkdata(self, bulk_data):
        self._logger.info("Collecting Data (%d)",len(bulk_data))
        self.es.add_bulk_document(self.indexname, bulk_data)

    def pushdata(self, data_dict):
        self._logger.info("Pushing  data ...")
        self.es.add_document(self.indexname,self.typename, data_dict, data_dict.keys()) 

    def searchresult(self, query):
        self._logger.info("Searching results ...")
        res = self.es.search_result(self.indexname, self.typename, query)
        resultlist = []
        for doc in res['hits']['hits']:
            resultmap = {}
            resultmap.setdefault('_id', doc['_id'])
            resultmap.setdefault('_source', doc['_source']) 
            resultlist.add(resultmap)
        return resultlist

    def scanresult(self, query):
        pagelist = self.es.scan_result_sync(self.indexname, self.typename, query)
        resultlist = []
        for page in pagelist:
            for doc in page['hits']['hits']:
                resultmap = {}
                resultmap.setdefault('_id', doc['_id'])
                resultmap.setdefault('_source', doc['_source'])
                resultlist.add(resultmap)
        return resultlist


    def _flogger(self):

        self._logger = logging.getLogger('ElasticSearchDriver')
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

