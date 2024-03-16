from dotenv import load_dotenv
import os
import pandas as pd
from Clases.CustomException import CustomException
from Clases.Catalogue import Catalogue
load_dotenv(os.path.join( os.getcwd(),'Clases/envs.txt'))

class CatalogUtility(Catalogue):
    
    
    def __init__(self, objConnMysql, lista_culumns_cat,  dic_columns_id,   dic_columns_vs_cat, dic_columns_cat  ):
        super().__init__( lista_culumns_cat,  dic_columns_id,   dic_columns_vs_cat )
        self.__dic_columns_cat = dic_columns_cat 
        self.objConnMysql      = objConnMysql
        print(self.get_lista_culumns_cat())
        print(self.get_dic_columns_id())
        print(self.get_dic_columns_vs_cat())
        print(self.__dic_columns_cat)
        
    def get_dic_columns_cat(self):
        return self.__dic_columns_cat
    
   

    def _get_dic_dfs_data_cat(self):
        #objConnMysql     =  ConnectMySQL()
        dic_dfs_cat_tab = {}
        for name_df, name_cat in (self.get_dic_columns_vs_cat()).items():
            print("tipo de dato name_cat es una: ",type(name_cat))
            res_cat = self.objConnMysql.get_catalogs_(  str(name_cat[0]) , self.__dic_columns_cat[name_df] )
            dic_dfs_cat_tab[f"{name_df}"] = res_cat if res_cat is not None else pd.DataFrame([],columns=self.__dic_columns_cat[name_df])
            #extract_data_uno tiene el close
        return dic_dfs_cat_tab
    
    def _update( self,df_result,dic_dfs_cat_tab_origen=None,dic_columns_modify=None):
        
        # obtener valores unicos de las columnas catalogo del resultado final
        dic_dfs_cat_result   =   self.get_dic_values_unique_df(  df_result  )
        
        ##print("dic_dfs_cat_result : ",dic_dfs_cat_result)
        ## leer las tablas catalogo
        if not dic_dfs_cat_tab_origen:
            dic_dfs_cat_tab_origen    =  self._get_dic_dfs_data_cat()
        
        ##print("dic_dfs_cat_tab_origen : ",dic_dfs_cat_tab_origen )
        
        # comparar los datos viejos del catalogo contra los nuevos datos del resultado procesado
        dic_dfs_cat_update        =  self.get_dic_dfs_cat_update(dic_dfs_cat_tab_origen, dic_dfs_cat_result)
        
        # aqui poner las columnas a modificar con algun tipo de valor
        
        try:                  
            # cargar los datos nuevos a los catalogos correspondientes
            for name_df_cat, df in dic_dfs_cat_update.items():
                    if name_df_cat in self.get_dic_columns_id():
                            if name_df_cat in self.get_dic_columns_vs_cat():
                                columns = self.get_dic_columns_id()[name_df_cat]
                                tab_cat = str(self.get_dic_columns_vs_cat()[name_df_cat][0])
                                print(f"Actualizando la tabla catalogo { tab_cat} referente a columas {columns}")
                                print(f"El df '{name_df_cat}' existe y su columna es: {columns} y su valor es {df.head(1)}")
                                #conecta interno
                                #validar de nuevo si no existe esos valores en la tabla si no los va a insertar de nuevo
                                
                                # aqui colomar las columas que se tiene que modificar a otro tipo d evalor
                                if dic_columns_modify:
                                     for type_col,columns in dic_columns_modify.items():
                                        #print(tipe_col,columns)
                                        df[columns] = df[columns].astype(type_col)
                                    
                                if df.empty:
                                    print("El DataFrame está vacío")
                                    print(f"El df '{name_df_cat}' y su valor es {df.head(1)}")
                                else:
                                    self.objConnMysql.load_data_chemy(df,tab_cat)
                                #desconecta interno
        except Exception as e:
            raise CustomException('Error al realizar actualizacion de catalogos',e)
           
        
 
    def _update_dataframe_with_ids(self, df_result,dic_dfs_cat_up_tab=None):
        try:
            if not dic_dfs_cat_up_tab:
                dic_dfs_cat_up_tab   =  self._get_dic_dfs_data_cat()
                
            df_result_by_ids     =  self.get_df_update_by_ids(dic_dfs_cat_up_tab, df_result)
            return df_result_by_ids
        except Exception as e:
            raise CustomException('Error al actualizar con ids la informacion de tipo catalogo',e)  