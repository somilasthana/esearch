import sys
import PConstant

class DummySchema(object):

    def __init__(self):
        self.request_body = {}

    def schema():
        return self.request_body

class TrialSchema(object):

    def __init__(self):

        self.request_body = { 
             "settings": {
                 "analysis": {
                     "filter": {
                         "english_stop": {
                             "type":       "stop",
                             "stopwords":  "_english_" 
                         },
                         "english_keywords": {
                             "type": "keyword_marker", "keywords": ["bill"] 
                         },
                         "english_stemmer": {
                             "type":       "stemmer",
                             "language":   "english"
                         },
                         "english_possessive_stemmer": {
                             "type":       "stemmer",
                             "language":   "possessive_english"
                         }
                     },
                     "analyzer": {
                         "english": {
                             "tokenizer":  "standard",
                                 "filter": [
                                     "english_possessive_stemmer",
                                     "lowercase",
                                     "english_stop",
                                     "english_keywords",
                                     "english_stemmer"
                                 ]
                         }
                     }
                }
             }, 

             "mappings" : {
                 "testarticles" : {
                     "properties" : {
	                "track_url": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "articlelink": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "versionNo": {
                            "type": "integer",
                            "index": "no"
                        },	
                        "keywords": {
                            "type": "string"
                        },
                        "childCategory": {
                            "type": "string"
                        },
                        "contributor": {
                            "type": "string"
                        },
                        "section" : {
                            "type": "string"
                        },
                        "subheading" : {
                            "type": "string"
                        },
                        "source" : {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "issueDate" : {
                            "type": "date"
                        },
			"body": {
                            "type": "string",
		            "analyzer": "english"
                        },
                        "parentCategory": {
                            "type": "string"
                        },
                        "images" : {
                            "type": "string"
                        },
                        "publishDate" : {
                            "type": "date"
                        },
                        "slug" : {
                            "type": "string" 
                        },
                        "publisher": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "channelslno": {
                            "type": "string",
                             "index": "not_analyzed"
                        },
                        "articleCreated": {
                            "type": "date"
                        },
                        "url": {
                            "type": "string"  
                        },
                        "articleid": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "articleTitle": {
                            "type": "string"
                        }
      
                     }
                 }
             }
        }

    def schema(self):
        return self.request_body


class ESDatabaseMetaStore(object):

    def get_request_body(self, operation):
        schemaobject = DummySchema()    
        if operation == PConstant.PConstant.TRIALDATA_SCHEMA.value:
            schemaobject =  TrialSchema()     
    
        return schemaobject.schema()

