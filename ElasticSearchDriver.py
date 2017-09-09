import sys
import PConstant
import PConfig
import ElasticSearchClient
import ESDatabaseMetaStore
import logging
import json

class ElasticSearchDriver(object):

    def __init__(self, indexname, typename):
        self.es = ElasticSearchClient.ElasticSearchClient()
        self.esdb = ESDatabaseMetaStore.ESDatabaseMetaStore()
        self.indexname = indexname
        self.typename = typename
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

    def searchresult(self):
        self._logger.info("Searching results ... index=%s type=%s", self.indexname, self.typename)
        queryhandle  = {}
        res = self.es.search_result(self.indexname, self.typename, queryhandle)
        resultlist = []
        for doc in res['hits']['hits']:
            resultmap = {}
            resultmap.setdefault('_id', doc['_id'])
            resultmap.setdefault('_source', doc['_source'])
            resultlist.append(resultmap)
        return resultlist


    def searchresult(self, query):
        self._logger.info("Searching results ... index=%s type=%s", self.indexname, self.typename)
        self._logger.info("Query '%s'", query)
        #queryhandle = { 'query' : json.loads(query) }
        #jquery = json.loads(query)
        #queryhandle = {"query": {"match": {"body": "McGrath"}}}
        queryhandle = {"query": {"match": query}}
        self._logger.info("Queryi Handle '%s'", json.dumps(queryhandle))
        res = self.es.search_result(self.indexname, self.typename, queryhandle)
        resultlist = []
        for doc in res['hits']['hits']:
            resultmap = {}
            resultmap.setdefault('_id', doc['_id'])
            resultmap.setdefault('_source', doc['_source']) 
            resultlist.append(resultmap)
        return resultlist

    def scanresult(self, query):

        #queryhandle = {"query": {"match": query}}
        queryhandle = query
        pagelist = self.es.scan_result_sync(self.indexname, self.typename, query)
        resultlist = []

        self._logger.info("Scan Handle '%s' '%s' '%s'", self.indexname, self.typename, queryhandle)
        for page in pagelist:
            for doc in page:
                resultmap = {}
                resultmap.setdefault('_id', doc['_id'])
                resultmap.setdefault('_source', doc['_source'])
                resultlist.append(resultmap)
        return resultlist


    def _flogger(self):

        self._logger = logging.getLogger('ElasticSearchDriver')
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

