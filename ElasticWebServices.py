import json
import logging
from wsgiref import simple_server 
import falcon
import sys
import ElasticSearchDriver

def token_is_valid(token, user_id):
    return True  # for now

def auth(req, resp, params):
    # Alternatively, do this in middleware
    token = req.get_header('X-Auth-Token')

    if token is None:
        raise falcon.HTTPUnauthorized('Auth token required',
                                      'Please provide an auth token '
                                      'as part of the request',
                                      'http://docs.example.com/auth')

    if not token_is_valid(token, params['user_id']):
        raise falcon.HTTPUnauthorized('Authentication required',
                                      'The provided auth token is '
                                      'not valid. Please request a '
                                      'new token and try again.',
                                      'http://docs.example.com/auth')


def check_media_type(req, resp, params):
    if not req.client_accepts_json:
        raise falcon.HTTPUnsupportedMediaType(
            'Media Type not Supported',
            'This API only supports the JSON media type.',
            'http://docs.examples.com/api/json')


class ElasticSearchService:

    def __init__(self):
        self.logger = logging.getLogger('elasticsearchresource.' + __name__)

    def on_get(self, req, resp):
        indexname = req.get_param('index') or 'default'
        typename = req.get_param('typev') or 'default'
        results = {}
        try:
            self.es = ElasticSearchDriver.ElasticSearchDriver(indexname, typename) 
            results = self.es.searchresult()
        except Exception as ex:
            self.logger.error(ex)
            description = ('Elastic Search Failed')
            raise falcon.HTTPServiceUnavailable('Service Outage',description,30)

        resp.status = falcon.HTTP_200
        resp.body = json.dumps(results)

    def on_post(self, req, resp):
        try:
            raw_json = req.stream.read()
        except Exception:
            raise falcon.HTTPError(falcon.HTTP_748,
                                   'Read Error',
                                   'Could not read the request body. Must be '
                                   'them ponies again.')

        try:
            inputdata = json.loads(raw_json, 'utf-8')
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect.')
        indexname = 'default'
        if 'index' in inputdata:
            indexname = inputdata['index']
        typename = 'default'
        if 'typev' in inputdata:
            typename = inputdata['typev']
        querystringhandle = {}
        if 'query' in inputdata:
            querystringhandle = inputdata['query']

        results = {}
        try:
            self.es = ElasticSearchDriver.ElasticSearchDriver(indexname, typename)
            results = self.es.searchresult(querystringhandle)
        except Exception as ex:
            self.logger.error(ex)
            description = ('Elastic Search Failed')
            raise falcon.HTTPServiceUnavailable('Service Outage',description,30)

        resp.status = falcon.HTTP_201
        finalresult = { 'status': 'OK', 'decs': 'QUERY SUCCEEDED' , 'result': results}
        resp.body =  json.dumps(finalresult)


class ElasticSearchEnhancedService:

    def on_post(self, req, resp):
        try:
            raw_json = req.stream.read()
        except Exception:
            raise falcon.HTTPError(falcon.HTTP_748,
                                   'Read Error',
                                   'Could not read the request body. Must be '
                                   'them ponies again.')

        try:
            inputdata = json.loads(raw_json, 'utf-8')
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect.')
        indexname = 'default'
        if 'indexname' in inputdata:
            indexname = inputdata['indexname']
        typename = 'default'
        if 'typename' in inputdata:
            typename = inputdata['typename']
        try:
            data_dict = inputdata['data']
        except Exception as ex:
            self.logger.error(ex)
            description = ('data')
            raise falcon.HTTPServiceUnavailable('Insert Requires data field',description,30)

        try:
            self.es = ElasticSearchDriver.ElasticSearchDriver(indexname, typename)
            if isinstance(data_dict, dict):
                self.es.pushdata(data_dict)
            else:
                for dd in data_dict:
                    self.es.pushdata(dd)     
        except Exception as ex:
            self.logger.error(ex)
            description = ('Elastic Search Failed')
            raise falcon.HTTPServiceUnavailable('Service Outage',description,30)

        resp.status = falcon.HTTP_201
        result = { 'status': 'OK', 'decs': 'INSERT SUCCEEDED' } 
        resp.body =  json.dumps(result)


#wsgi_app = api = falcon.API(before=[auth, check_media_type])
wsgi_app = api = falcon.API()
ess = ElasticSearchService()

api.add_route('/simpledbfunc', ess) 
app = application = api

# Useful for debugging problems in your API; works with pdb.set_trace()
if __name__ == '__main__':
    httpd = simple_server.make_server('0.0.0.0', 8000, app)
    httpd.serve_forever()
#
# gunicorn ElasticWebServices:app --debug --log-level info --access-logfile ./gunicorn-access.log --error-logfile ./gunicorn-error.log
#
#
