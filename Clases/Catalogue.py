import pandas as pd
from dotenv import load_dotenv
import os
from Clases.CustomException import CustomException
load_dotenv(os.path.join( os.getcwd(),'Clases/.envs'))
import warnings

class Catalogue:
    
    def __init__(self,lista_culumns_cat,dic_columns_id, dic_columns_vs_cat):
        self.__lista_culumns_cat  =  lista_culumns_cat
        self.__dic_columns_id     =  dic_columns_id
        self.__dic_columns_vs_cat =  dic_columns_vs_cat
        
    
    def get_lista_culumns_cat(self):
        return self.__lista_culumns_cat
    
    def get_dic_columns_id(self):
        return self.__dic_columns_id
    
    def get_dic_columns_vs_cat(self):
        return self.__dic_columns_vs_cat
    
    def createKey_df(self,df, columns):
        try:
            #with warnings.catch_warnings():
             #   warnings.simplefilter("ignore", category=pd.core.common.SettingWithCopyWarning)

            values = list(dict.fromkeys(columns))
            if len(df) != 0:
                df['key'] = df[values].astype(str).apply('-'.join, axis=1)
                df["key"] = df["key"].str.strip()
                df["key"] = df["key"].str.replace(' ', '')
                df["key"] = df["key"].str.upper()
                #return df
        except Exception as e:
            print("Error al generar llave:", e)
            
    def deleteKey_df(self,df):
        try:
            #with warnings.catch_warnings():
             #   warnings.simplefilter("ignore", category=pd.core.common.SettingWithCopyWarning)

            if len(df) != 0:
                df.drop('key', axis=1,inplace = True) 
        except Exception as e:
            print("Error al eliminar llave:", e)


    def convert_float_columns_to_str(self,df, float_columns):
        for column in float_columns:
            if df[column].dtype == 'float64':
                df[column] = df[column].astype(str)
        return df 
    
    def get_dic_values_unique_df(self,df_result):
        #for col in self.__lista_culumns_cat:
         #   df_result = self.convert_float_columns_to_str(df_result, self.__dic_columns_id['df_' + col])
        #dic_val_uniq_by_col = {col: df_result[col].unique() for col in self.__lista_culumns_cat}
        #revisar por df_x sus columnas y hacer los unicos valores
        print(df_result.dtypes)
        
        dic_val_uniq_by_col = {col: df_result[self.__dic_columns_id['df_' + col]].groupby(by=self.__dic_columns_id['df_' + col],dropna=False,as_index=False).first().sort_values(by=self.__dic_columns_id['df_' + col]).reset_index(drop=True)  for col in self.__lista_culumns_cat}
        
        dic_dfs_cat_result = {}
        for clave, valor in dic_val_uniq_by_col.items():
            print("clave",clave, "....valor:",valor,"tipovalor:",type(valor))
            dic_dfs_cat_result[f"df_{clave}"] =  valor #pd.DataFrame({clave: valor}, columns=[clave])
        print("imprimiendo dic_dfs_cat_result:",dic_dfs_cat_result)
        return dic_dfs_cat_result
    
    def get_dic_dfs_cat_update(self,dic_dfs_cat_tab, dic_dfs_cat_result):
    
        def get_df_new_cat(df_cat,df_cat_result,columns):
            merged_df  = df_cat.merge(df_cat_result, on=columns, how='right',indicator=True)
            df_new_cat = merged_df[merged_df['_merge']=="right_only"][columns].reset_index(drop=True)
    
            #df_new_cat =  df_new_cat.drop(columns=['_merge'])
            return pd.DataFrame(df_new_cat)

        dic_dfs_new_cat = {}
        for name_df, df in dic_dfs_cat_result.items():
            print("name_df",display(df.head(1)))
            try:
                if name_df in self.__dic_columns_id:
                    if name_df in self.__dic_columns_vs_cat:
                        columns_union = self.__dic_columns_id[name_df]
                        catalogo = self.__dic_columns_vs_cat[name_df]
                        print(f"Actualizar catalogo {catalogo} referente a columas {columns_union}")
                        print(f"El df '{ name_df}' existe y su columna union es: {columns_union} y su valor es {dic_dfs_cat_tab[name_df].head(1)}")
                        df_new_cat = get_df_new_cat(dic_dfs_cat_tab[name_df],dic_dfs_cat_result[name_df],columns_union)
                        dic_dfs_new_cat[name_df] = df_new_cat
                        display(df_new_cat.head(1) )
                else:
                    pass
                    print(f"La clave '{name_df}' no existe en el diccionario")
            except Exception as e:
                if name_df in self.__dic_columns_vs_cat:
                    catalogo = self.__dic_columns_vs_cat[name_df]
                    print(f"Existe un error al actualizar catalogo {catalogo}, por error: ",e)
                raise CustomException(f"Existe un error al actualizar catalogo {catalogo}, por error:",e) 

        return dic_dfs_new_cat
    
    def get_df_update_by_ids(self,dic_dfs_cat_up_tab,df_result_all):
    
        def remplace_dataframes_by_id(df_cat_up,df_result,columns):
            
            merged_df    = df_cat_up.merge(df_result, on=columns, how='inner',indicator=True)
            display(merged_df.head(1))
            df_result_id = merged_df[merged_df['_merge']=="both"].reset_index(drop=True)
            display(df_result_id.head(1))
            df_result_id_nofound = merged_df[merged_df['_merge']=="right_only"].reset_index(drop=True)
            print("no encontrados....mmmmmmmmmmmmmmmmmmmm")
            display( df_result_id_nofound.head(1))
            df_result_id =  df_result_id.drop(columns=['_merge'])
            df_result_id = df_result_id.drop(columns=columns)
            
            display(df_result_id.head(1))
            return pd.DataFrame(df_result_id)

        display(df_result_all.head(1))
        for name_df_cat, df_cat_up in dic_dfs_cat_up_tab.items():
            try:
                if name_df_cat in self.__dic_columns_id:
                    if name_df_cat in self.__dic_columns_vs_cat:
                        columns_union = self.__dic_columns_id[name_df_cat]
                        catalogo = self.__dic_columns_vs_cat[name_df_cat]
                        print(f"Remplazarndo ids del catalogo {catalogo}  en el resultado procesado, referente a columas {columns_union}")
                        print(f"El df '{name_df_cat}' existe y su columna union es: {columns_union} y su valor es {df_cat_up.head(1)}")
                        df_result_all = remplace_dataframes_by_id(df_cat_up,df_result_all,columns_union)
                        display(df_result_all.head(1))
                else:
                    pass
                    print(f"La clave '{name_df}' no existe en el diccionario")
            except Exception as e:
                if name_df in self.__dic_columns_vs_cat:
                    catalogo = self.__dic_columns_vs_cat[name_df_cat]
                    print(f"Existe un error al actualizar los ids del  catalogo {catalogo} en el resultado procesado, por error: ",e)
                raise CustomException(f"Existe un error al actualizar los ids del  catalogo {catalogo} en el resultado procesado, por error: ",e) 

        return df_result_all