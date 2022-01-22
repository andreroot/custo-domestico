import pandas as pd
import numpy as np
import datetime as dt

class AnaliseDataFrame():

    def estudo_lista_compras(self, df_extrato):
        #df_extrato[[ "item","codigo","descricao","tipo_unid","qtd","vl_unitario","vl_item","forma_pagamento","valor_total","data_compra","mercado", "dt_mes_base", "process_time"]]
        df_extrato = df_extrato[[ "descricao","qtd","vl_unitario","data_compra"]]
        freq = df_extrato.loc[df_extrato['descricao']=='UVA CRIMSON $S 450G'].groupby(['descricao']).count() 
        print(freq)
        freq = df_extrato.loc[df_extrato['descricao']=='UVA CRIMSON $S 450G'].groupby(['descricao']).sum('qtd') 
        print(freq)
        freq = df_extrato[df_extrato['descricao'].str.contains("UVA")].groupby(['descricao']).count() 
        print(freq)   
        freq = df_extrato[df_extrato['descricao'].str.contains("UVA")].groupby(['descricao']).sum('qtd') 
        print(freq)             
        #freq = df_extrato[df_extrato['descricao'].str.contains("UVA")].value_counts().to_dict()
        #print(freq)
      