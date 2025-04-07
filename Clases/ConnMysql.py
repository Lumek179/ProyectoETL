#!/usr/bin/env python
# coding: utf-8
# comentario nuevo
import os

import pandas as pd
from Clases.CustomException import CustomException
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

class ConnectMySQL():
    
    def __init__(self, host, bd, user, password, port):
    
        self.host               =  host
        self.bd                 =  bd
        self.user               =  user
        self.port               =  port
        self.password           =  password
        self.string_conn        =  f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:3310/{self.bd}"
        #"mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(user, password, host, port, database)
        self.metadata = MetaData()
        self.batch_size     = 100000
        print(self.host , self.bd, self.user, self.port, self.password ,self.string_conn )
        try:
            self.engine      =  create_engine(self.string_conn)
        except Exception as e:
            raise CustomException('Error al crear el engine',e)   

    ## Función de conexión a MySQL
    def create_engine(self):
        try:
            self.engine      =  create_engine(self.string_conn)
            #return create_engine(self.string_conn)   # este valor si quiero hacer multiples engine lo puedo descomentar y modificar
        except Exception as e:
            raise CustomException('Error al conectar con la base de datos',e) 
        
    def set_query(  self,  search_query ):
        self.__query  =  search_query
        
    def get_query(self):
        return self.__query
    
    def set_batch(  self,  batch_size ):
        self.batch_size   =  batch_size
        
    def get_batch(self):
        return self.batch_size 
    
    def generar_query_insert(self,table, columns):
        """
        Genera dinámicamente un query de inserción para una tabla dada y una lista de columnas.

        Args:
        - tabla: nombre de la tabla en la que se van a insertar los datos.
        - columnas: lista de nombres de columnas en la tabla.

        Returns:
        - query: query de inserción generado dinámicamente.
        """
        num_columns = len(columns)
        columns_str = ', '.join(columns)
        values_str = ', '.join(['%s'] * num_columns)
        self.__query = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str})"
    
    
    def close_connect(self):  
        try:
            self.__conn.close()
        except Exception as e:
            raise CustomException('Error al cerrar la conexión con la base de datos',e)  
  
            
    def extract_data(self):
        #self.create_engine()
        df  = pd.read_sql(self.__query , con=self.engine.connect() ) 
        return df
    
    def load_data(self,df, tbl_name):
        pass
        #df.to_sql(tbl_name, self.engine , if_exists='replace')
        
    def load_data_chemy_masive(self, df, name_table_dest,batch_size=None):
        if batch_size != None:
            self.batch_size = batch_size
        
        df = df.reset_index(drop=True)
        Session = sessionmaker(bind=self.engine)
        table_dest = Table(name_table_dest, self.metadata, autoload=True, autoload_with=self.engine)
        print(table_dest)
        try:
            batch_size = 100000  # Tamaño del lote
                
            for i in range(0, len(df), batch_size):
                batch = df.iloc[ i : i + batch_size ].to_dict(orient='records')
                batch_size_current = len(batch)
                print("batch:", batch_size_current)
                # Insertar datos utilizando la sesión
                with Session() as se:
                    se.execute(table_dest.insert(), batch)
                    se.commit()  # Confirmar la transacción
        except SQLAlchemyError as e:
            print("Error:", e)
            if se:
                se.rollback()  # Revertir la transacción en caso de error
        except Exception as e:
            print("Error:", e)

     
    def load_data_chemy(self,df,name_table_dest):
        Session = sessionmaker(bind=self.engine)
        table_dest = Table(name_table_dest, self.metadata , autoload=True, autoload_with=self.engine)
        print(table_dest)
        try:
            datos = df.to_dict(orient='records')
            # Insertar datos utilizando la sesión
            with Session() as se:
                se.execute(table_dest.insert(), datos)
                se.commit()  # Confirmar la transacción
        except SQLAlchemyError as e:
            print("Error:", e)
            if se:
                se.rollback()  # Revertir la transacción en caso de error  
        except Exception as e:
            print("Error:", e)

    def load_data_chemy_connect(self,df,name_table_dest):
        table_dest = Table(name_table_dest, self.metadata , autoload=True, autoload_with=self.engine)
        print(table_dest)
        try:
            datos = df.to_dict(orient='records')  
            with self.engine.connect() as conn:
                conn.execute(table_dest.insert(), datos)
        except SQLAlchemyError as e:
            conn.rollback()  # Revertir la transacción en caso de error
            print("Error:", e)
        except Exception as e:
            print("Error:", e)
            
    def iterable_batch_chemy( self ):
        while True:
            rows = self.result.fetchmany(self.batch_size)
            if not rows:
                print("entro a no rows")
                print("rows",rows)
                break
            yield rows
            
    def extract_data_chemy(self):
        try:
            with self.Session() as session:
                self.result = session.execute(self.__query)
                self.column_names = list(self.result.keys())
                print("column_names:",self.column_names )
                for batch in self.iterable_batch_chemy():
                    pivote = pd.DataFrame(batch,columns= self.column_names) 
                    yield pivote
        except SQLAlchemyError as e:
            print("Error:", e)
        except Exception as e:
            print("Error:", e)
        
        
    def extract_data_all(self,batch_size=None):
        if batch_size !=None:
            self.batch_size = batch_size
        
        self.Session = sessionmaker(bind=self.engine)
        df_list =[]
        for pivote in self.extract_data_chemy():
            #print("pivote:--------")
            #print(pivote.shape)
            if not pivote.empty:
                df_list.append(pivote) 
        if df_list:  # Verificar si df_list contiene al menos un DataFrame
            return pd.concat(df_list, ignore_index=True)
        else:
            return pd.DataFrame([],columns= self.column_names)  # Devolver un DataFrame vacío si df_list está vacío
    
    
    def get_catalogs_(self,catalog,columns):
        cols= ','.join(columns)
        try:
            df_result=None
            self.__query    =  f'Select {cols} from {catalog};'
            print("query:",self.__query )
            df_result  =  self.extract_data_all()
            print(type(df_result))
            print(df_result.shape)
            
            display("df_result",df_result)
            print("longitud:",len(df_result))
            #print("result:",result)
            #print("columns",columns)
            return df_result
        except CustomException as e:
            raise e
