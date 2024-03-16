#!/usr/bin/env python
# coding: utf-8

# In[2]:


#!/usr/bin/env python
import elasticsearch
from elasticsearch import Elasticsearch, helpers, exceptions
from ssl import create_default_context
import json
import pandas as pd

class ConnectElasticsearch:

    __time_scroll                 =  '1m'
    __resp                        =  ''
    __scroll                      =  '1s'  # time value for search
    __rest_total_hits_as_int      =  True
    __scheme                      =  "https"
    __context_dir                 =  "/etc/pki/ca-trust/source/anchors/elastic-stack-ca.pem"
    
    """ 
    Se define la clase de conexion a Elasticsearch
    """   
    def __init__( self, context_dir  =  None, user  =  'elastic', pwd  =  'elastic', port  =  9200,  ):
        
        if not context_dir is None :
            self.__context                 =  create_default_context(  cafile  =  context_dir  )
            self.__context.check_hostname  =  False
            
        self.__user                   =  user
        self.__pwd                    =  pwd
        self.__http_auth              =  (  self.__user, self.__pwd )
        self.__port                   =  port
        self.__ips                    =  [  '192.168.200.23', '192.168.200.24', '192.168.200.25'  ]

    """
    metodos de get y set
    """  

    def get_indexName(self):
        return self.indexName
    
    def set_indexName( self,  indexName  ):
         self.indexName  =  indexName
            
                
    def get_es(self):
        return self.__es
    
    def set_result(self):
        
        self.__resp   = self.__es.search(  index       = self.nameIndex,
                                           body        = self.__query,
                                           scroll      = self.__time_scroll # time value for search 1 milisecond
                                        )
    def set_id_scroll(self):
        self.__idScroll  =  self.__resp [ '_scroll_id' ] 
    
    def get_id_scroll(self):
        return self.__idScroll  
    
    def show_id_scroll(self):
        print(  "idScroll:"  ,  self.get_id_scroll(  ) )
        
    def get_hits_size_ini(self):
        return len(  self.__resp[ "hits" ][ "hits" ]  )
    
    def get_total_hits(self):
        return self.__resp[ "hits" ][ "total" ][ "value" ] 
    
    def get_result_scroll(self):
        
        return self.__es.scroll(
                                 scroll_id              = self.__idScroll,
                                 scroll                 = self.__scroll,
                                 rest_total_hits_as_int = self.__rest_total_hits_as_int
                                )["hits"]["hits"]

    
    def get_aggs(self):
        
        lenAggs  =  len(  self.__resp[ "aggregations" ][ "groupby" ][ "buckets" ]  )
        
        if lenAggs  !=  0:
            
            list_aggs  =  self.__resp[ "aggregations" ][ "groupby" ][ "buckets" ]
            
            if "after_key" in resp[ "aggregations" ][ "groupby" ]:
                
                after  =  resp[ "aggregations" ][ "groupby" ][ "after_key" ]
            else:
                after  =  { }
        else:
            list_aggs  =  [ ]
            after      =  { }

        return list_aggs, after
    
    def set_query(  self,  search_query  ):
        self.__query  =  search_query
        
    def get_query(self):
        return self.__query
        
    def set_reindex_query(  self,  reindex_query  ):
        self.__queryReindex  =  reindex_query
    
    """
    metodos print
    """
    def show_total_hits(self):
        print ( "total_hits_consulta:",  self.__resp[ "hits" ][ "total" ][ "value" ]  )
        
    def show_total_hits_ini(self):
        print ( "total_hits_inicial:",  len( self.__resp[ "hits" ][ "hits" ]  )  )        
    
    """
    metodos de conexion y desconexion
    """
    def connect(self):
        
        try:
            self.__es  =  Elasticsearch( self.__ips,
                                         http_auth    =  self.__http_auth,
                                         scheme       =  self.__scheme,
                                         port         =  self.__port,
                                         ssl_context  =  self.__context)
            self.info  =  self.__es.info()
            
        except exceptions.ConnectionError as err:
            
            print( 'EC error Connect: ', err)
            self.__es  =  None
            
    def close_connect(self):
        
        try:
            
            self.__es.close()
            
        except exceptions as err:
            
            print( 'EC error CloseConnect: ', err)

    """
    metodos de bulk: carga, actualizacion, eliminacion
    """
    def bulk_data(  self,  json_list  ):
        
        for doc in json_list:
            _index= doc["indexName"]
            status_delete_key= doc.pop("indexName",None)
            if status_delete_key != None:
                yield {
                        "_index"   :  _index,
                        "_source"  :  doc
                }
            
    def bulk_data_update(  self,  json_list  ):
        
        for doc in json_list:
            
            yield {
                     "_op_type"  :  "update",
                     "_index"    :  self.nameIndex,
                     "_id"       :  doc[  "_id"  ],
                     "doc" :
                           { 
                             "attachment_filename"        :  doc["attachment_filename"],
                             "content_attachment.filename":  doc["content_attachment.filename"],
                             "status_read"                :  doc["status_read"],
                             "unique_id_file"             :  doc["unique_id_file"]
                           }
                  }
    def bulk_data_update_doc(  self,  json_list  ):
        #_doc_type = '_doc' no es necesario lo genera en automatico como _doc
        for doc in json_list:
            
            _id                   =  doc[ "_id" ]
            status_delete_key     =  doc.pop( "_id" , None )
            
            if status_delete_key  !=  None:
                
                yield {
                        "_op_type" :  "update",
                        "_index"   :  self.nameIndex,
                        "_id"      :  _id,
                        "doc"      :  doc
                }
    def bulk_data_delete(  self,  json_list  ):
        #"doc":json.loads(doc["doc"])
        for doc in json_list:
            # use un generador de rendimiento para que los datos no se carguen en la memoria
            yield {
                    "_op_type"  :  "delete",
                    "_index"    :  self.nameIndex,
                    "_id"       :  doc[ "_id" ]
            }
            
    
    """
    metodos de: carga, actualizacion, eliminacion
    """
            
    def load_data(  self,  final_json_list, indexName  ):
        self.nameIndex  =  indexName
        try:
            
            for success, action in helpers.streaming_bulk( self.__es, 
                                                           self.bulk_data(  final_json_list  ),
                                                           raise_on_error      =  True,
                                                           raise_on_exception  =  False,
                                                           yield_ok            =  False,):

                if not success:
                    
                    print( 'A document failed:', action )
        except Exception as e:
            
            print( "\nERROR Load Data:" , e  )

            
    def update_data(  self,  final_json_list  ):
        self.nameIndex  =  indexName
        try:
            
            for success, action in helpers.streaming_bulk(self.__es,
                                                          self.bulk_data_update( final_json_list ),
                                                          raise_on_error      =  True,
                                                          raise_on_exception  =  False,
                                                          yield_ok            =  False,):
            
                if not success:
                    
                    print( 'A document failed:' ,  action  )
        except Exception as e:
            
            print(  "\nERROR Load Data Update:"  ,  e  )
            
            
    def update_doc(  self,  final_json_list,  indexName  ):
        self.nameIndex  =  indexName
        try:
            for success, action in helpers.streaming_bulk(  self.__es,
                                                            self.bulk_data_update_doc(  final_json_list  ),
                                                            raise_on_error      =  True,
                                                            raise_on_exception  =  False,
                                                            yield_ok            =  False,):
                #print("ok:",success)
                #print(" action:", action)
                #pass
                if not success:
                    print(  'A document failed:',  action  )
                    
        except Exception as e:
            
            print(  "\nERROR Load Data Update:",  e  )
            
            
    def delete_data(self,  final_json_list, indexName):
        self.nameIndex  =  indexName
        try:
            for success, action in helpers.streaming_bulk(self.__es,
                                                          self.bulk_data_delete(  final_json_list  ),
                                                          raise_on_error       =  True,
                                                          raise_on_exception   =  False,
                                                          yield_ok             =  False,):
                print(  "ok:"  ,  success  )
                #print(" action:", action)
                #pass
                if not success:
                    
                    print( 'A document failed:',  action )
        except Exception as e:
            
            print(  "\nERROR Load Data Delete:"  )
            print( e )## quitar
            #print(e) ## aqui se detiene si un documento en la lista no fue encontrado secuencialmente como va, podria fallar todo el bloque
        
    
    """
    metodos de reindexacion
    """
    def reindex(  self,  indexSource,  indexDest  ):# arreglar par mas datos de 10000
        
        print(  "Reindexando indice ",  indexSource,  " a " ,  indexDest  )
        
        total_success,  error  =  helpers.reindex(  client        =  self.__es,
                                                    source_index  =  indexSource, 
                                                    target_index  =  indexDest,
                                                    query         =  self.__queryReindex,
                                                    target_client =  None,
                                                    chunk_size    =  500,
                                                    scroll        =  '5m',
                                                    scan_kwargs   =  { },
                                                    bulk_kwargs   =  { } )
        
        print(  "total_docs_reindexados:",  total_success  )
        
        return total_success
    
    """
    metodos para limpiar scroll
    """
    def clear_scroll(self):
        
        self.__es.clear_scroll(  scroll_id  =  "_all"  )
        print(  "Se eliminaron todos los scrolls utilizados"  )
        
    def clear_id_scroll(self):
        
        body_scroll  =  {  "scroll_id"  :  "{0}".format(  self.__idScroll  )  }
        self.__es.clear_scroll(  body  =  body_scroll,  ignore  =  (  404,  400,  )  )
        print(  "Se elimino el scroll utilizado: "  ,  self.__idScroll  )
        
    
    """
    metodos para extraer hists de elasticsearch
    """

    def extract_list_hits(self,indexName): 
        self.nameIndex  =  indexName
        list_hits  =  [ ] 
        
        try:
            
            if self.__es  !=  None:
                
                self.set_result( )
                self.set_id_scroll()
                list_hits  =  self.__resp[ "hits" ][ "hits" ]
                self.show_total_hits( )
                self.show_total_hits_ini( )
                
                hits  =  1
                
                while hits  >  0:
                    otherRespHits  =  self.get_result_scroll( )
                    hits           =  len(  otherRespHits  )
                    print(  "Next_hits: "  ,  hits  )
                    list_hits.extend(  otherRespHits  )
                
                self.show_id_scroll( )
                self.clear_id_scroll( )
            
            else:
                print(  "Existe un error en la conexion a elasticsearch"  )
        
        except exceptions as err:
        
            print( 'EC error: ',  err  )
        
        finally:
            size  =  len(  list_hits  )
            print(  "total_final_hits:"  ,  size  )
            print("-----------------")
        return list_hits,size
    
    def delete_index(self, indexName):
        self.nameIndex  =  indexName
        self.__es.indices.delete(  index  =  self.nameIndex  )
    """
    metodos para convertir dataframe a json
    """
    def convert_df_to_json(  self,  df  ):
        listStr         =  df.to_json(  orient  =  "records"  )
        listJson        =  json.loads(  listStr )
        return listJson
    

