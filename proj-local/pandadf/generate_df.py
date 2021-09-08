import pandas as pd
from bucket.mov_file_bucket import MovimentacaoFileBucket
from drive.sheet import DriveSheet
import numpy as np
import datetime as dt
import warnings
import openpyxl

class GenerateDF():
   

    def csv_generate_df_depara_debito(self, csv_local):

        print("-> gerando dataframe baseado no csv (pasta local):", csv_local, "\n")
        df_depara = pd.read_csv(csv_local, sep=';',usecols=['de_para','valor'])#, sep='\t'

        print("-> remove duplicadas", "\n")
        df_depara_sem_dup = df_depara.drop_duplicates()
        
        return df_depara_sem_dup


    def csv_generate_df_custo(self, csv_local):

        print("-> gerando dataframe baseado no csv(separador ';' / decimal '.') (pasta local):", csv_local, "\n")
        df_custo = pd.read_csv(csv_local, sep=';', usecols=['tipo_custo','custo','valor_custo','dt_pagto_bq','dt_custo_bq'])#, sep='\t'
        #df_custo["valor_custo"] = df_custo["valor_custo"].astype(str)
        
        print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", csv_local, "\n")
        df_custo['dt_pagto_bq'] = pd.to_datetime(df_custo['dt_pagto_bq'])
        df_custo['dt_custo_bq'] = pd.to_datetime(df_custo['dt_custo_bq'])


        #print(df_custo["valor_custo"].dtypes, "\n")
        print(df_custo.dtypes, "\n")
        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", csv_local, "\n")
        df_custo["valor_custo"] = df_custo["valor_custo"].astype(str)
        #print(df_custo["valor_custo"].dtypes, "\n")
        #print(df_custo, "\n")
        df_custo["valor_custo"] = [x.replace(",", ".") for x in df_custo["valor_custo"]]

        df_custo["valor_custo"] = pd.to_numeric(df_custo["valor_custo"].fillna(0), errors="coerce")
        df_custo["valor_custo"] = df_custo["valor_custo"].map("{:.2f}".format)
        df_custo["valor_custo"] = df_custo["valor_custo"].astype(float)
        print(df_custo, "\n")
        print(df_custo.dtypes, "\n")
        #df_depara_sem_dup = df_depara.drop_duplicates()
        print("-> retorna dataframe tratado", "\n")

        return df_custo


    def csv_generate_df_recebido(self, csv_local):

        print("-> gerando dataframe baseado no csv(separador ';' / decimal '.') (pasta local):", csv_local, "\n")
        df_recebido = pd.read_csv(csv_local, sep=';', usecols=['dt_mes_base','dt_recebido','descricao','valor_recebido','valor_previsto'])#, sep='\t'
        
        print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", csv_local, "\n")
        df_recebido['dt_mes_base'] = pd.to_datetime(df_recebido['dt_mes_base'],format='%d/%m/%Y')
        df_recebido['dt_recebido'] = pd.to_datetime(df_recebido['dt_recebido'],format='%d/%m/%Y')


        #print(df_custo["valor_custo"].dtypes, "\n")
        #-> valor recebido
        print(df_recebido.dtypes, "\n")
        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", csv_local, "\n")
        df_recebido["valor_recebido"] = df_recebido["valor_recebido"].astype(str)
        #print(df_recebido, "\n")
        #print(df_custo, "\n")
        df_recebido["valor_recebido"] = [x.replace(",", ".") for x in df_recebido["valor_recebido"]]

        df_recebido["valor_recebido"] = pd.to_numeric(df_recebido["valor_recebido"].fillna(0), errors="coerce")
        #print(df_recebido, "\n")
        df_recebido["valor_recebido"] = df_recebido["valor_recebido"].map("{:.2f}".format)
        df_recebido["valor_recebido"] = df_recebido["valor_recebido"].astype(float)

        #-> valor previsto
        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", csv_local, "\n")
        df_recebido["valor_previsto"] = df_recebido["valor_previsto"].astype(str)
        #print(df_recebido, "\n")
        #print(df_custo, "\n")
        df_recebido["valor_previsto"] = [x.replace(",", ".") for x in df_recebido["valor_previsto"]]

        df_recebido["valor_previsto"] = pd.to_numeric(df_recebido["valor_previsto"].fillna(0), errors="coerce")
        #print(df_recebido, "\n")
        df_recebido["valor_previsto"] = df_recebido["valor_previsto"].map("{:.2f}".format)
        df_recebido["valor_previsto"] = df_recebido["valor_previsto"].astype(float)

        print(df_recebido, "\n")

        print(df_recebido.dtypes, "\n")
        #df_depara_sem_dup = df_depara.drop_duplicates()
        print("-> retorna dataframe tratado", "\n")
        #-> retorna dataframe
        return df_recebido

    def txt_generate_df_custo(self, txt_local):

        print("-> gerando dataframe baseado no csv(separador ';' / decimal '.') (pasta local):", txt_local, "\n")
        
        df_extrato = pd.read_csv(txt_local, sep=';', header=None)
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext" ]

        nome_csv = txt_local.replace(".txt", ".csv")
        df_extrato.to_csv(nome_csv, index=None)
        #df_custo = pd.read_csv(txt_local.replace(".txt", ".csv"), sep=';', usecols=['dt_mes_base','dt_recebido','descricao','valor_recebido'])#, sep='\t'
        

        print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", txt_local, "\n")

        #now = dt.datetime.now()
        #dt_ = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        #print(dt_.strftime("%Y-%m-%d"))
        #teste_f = dt.datetime(year=int(dt_.strftime("%Y")), month=8, day=1)
        df_extrato["tipo_custo"] = None

        df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')

        df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%Y")
        df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%m")
        df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[0]),month=int(df_extrato['mes'].iloc[0]), day=1)
        
        #print(df_dt_base, "\n")
        #print(mes)

        #print(df_custo_tt['dt_custo_bq'].iloc[0]) 
        #print(df_custo_tt['dt_custo_bq'].iloc[-1])
        #print(pd.to_datetime(df_custo_tt['dt_custo_bq'].iloc[0]).strftime("%Y")) 
        
        #data_base = df_custo_tt['dt_custo_bq'].iloc[0]
        #print(data_base[2:6])
        #print(data_base[4:6])

        df_extrato['dt_base'] = df_dt_base 
        #pd.DatetimeIndex(pd.to_datetime(df_custo_tt['dt_custo_bq'].iloc[0]).strftime("%Y"))
        #df_custo_tt['dt_custo_bq'] = pd.to_datetime(df_custo_tt['dt_custo_bq'])
        
        print(df_extrato, "\n")
        print(df_extrato.dtypes, "\n")

        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", txt_local, "\n")
        df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(str)
        #print(df_custo["valor_custo"].dtypes, "\n")
        #print(df_custo, "\n")
        df_extrato["valor_ext"] = [x.replace(",", ".") for x in df_extrato["valor_ext"]]
        

        df_extrato["valor_ext"] = pd.to_numeric(df_extrato["valor_ext"].fillna(0), errors="coerce")
        df_extrato["valor_ext"] = df_extrato["valor_ext"].map("{:.2f}".format)
        df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(float)

        print(df_extrato, "\n")


        #result = [f(x) for x in df_custo_tt['custo']]
        custo_de_para = 'csv/domestico/de_para_debito.csv'
        print("-> aplicar de-para custo", custo_de_para, "\n")

        df_custo_de_para= pd.read_csv(custo_de_para, sep=';', usecols=['de_para','valor'])

        for index_cc, row_cc in df_extrato.iterrows():
            print (row_cc["descricao"], index_cc)
            idx = index_cc
            if row_cc["descricao"].find('TAR PACOTE ITAU')>=0:
                print("primeira verificação de de-para custo => banco")
                var_tp = 'banco'
                df_extrato.at[idx,"tipo_custo"] = var_tp

            elif row_cc["descricao"].find('INT PAG TIT')>=0:
                print("primeira verificação de de-para custo => boleto")
                var_tp = 'boleto'
                df_extrato.at[idx,"tipo_custo"] = var_tp 

            elif row_cc["descricao"].find('DA  NEXTEL TELECOM')>=0:
                print("primeira verificação de de-para custo => celular")
                var_tp = 'celular'
                df_extrato.at[idx,"tipo_custo"] = var_tp    

            elif row_cc["descricao"].find('TBI')>=0:
                print("primeira verificação de de-para custo => poupança")
                var_tp = 'poupança'
                df_extrato.at[idx,"tipo_custo"] = var_tp  

            elif row_cc["descricao"].find('SAQUE')>=0:
                print("primeira verificação de de-para custo => saque")
                var_tp = 'saque'
                df_extrato.at[idx,"tipo_custo"] = var_tp     

            elif row_cc["descricao"].find('UNICEF')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'doação'
                df_extrato.at[idx,"tipo_custo"] = var_tp 

            elif ((row_cc["descricao"].find('DA  NEXTEL TELECOM') < 0)  and 
                    (row_cc["descricao"].find('INT PAG TIT') < 0)       and 
                    (row_cc["descricao"].find('TAR PACOTE ITAU') < 0)):
                print("segunda verificação de de-para custo")
                tipo_custo = df_custo_de_para.where(df_custo_de_para['de_para'] == row_cc["descricao"])
                #print(tipo_custo)
                var_tipo_custo = tipo_custo[(tipo_custo["valor"].notnull())]#.unique()
                print(len(var_tipo_custo.index))

                if len(var_tipo_custo.index)>0:

                    for index_dp, row_dp in var_tipo_custo.iterrows():
                        
                        if row_cc["descricao"] == row_dp['de_para']:
                            #print (row_cc["custo"])
                            #print (row_dp["de_para"], row_dp["valor"])
                            tipo_custo_x = row_dp["valor"]
                            print("for depara compara com custo => ",tipo_custo_x)
                            df_extrato.at[idx,"tipo_custo"] = tipo_custo_x
                            #print(tipo_custo_x)
                        else:
                            df_extrato.at[idx,"tipo_custo"] = "compras"
                else:
                    df_extrato.at[idx,"tipo_custo"] = "compras"
            else:
                df_extrato.at[idx,"tipo_custo"] = "compras"
        #print(type(var_tipo_custo))
        #print(var_tipo_custo.tostring())
        #print(np.fromstring(var_tipo_custo, dtype=str))
        '''
        for index_dp, row_dp in df_custo_de_para.iterrows():
            if row_cc["custo"].equals(row_dp['de_para']):
                print (row_cc["custo"])
                print (row_dp["de_para"], row_dp["valor"])
                tipo_custo = row_dp["valor"]
            else:
                tipo_custo = "compras"
                #df_custo_tt.at[index_cc,"tipo_custo"] = tipo_custo
            df_custo_tt["tipo_custo"] = tipo_custo
        '''
        #df_depara_sem_dup = df_depara.drop_duplicates()
        print("-> retorna dataframe tratado", "\n")

        df_extrato = df_extrato.where(df_extrato['valor_ext'] < 0)
        df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')==False]
        #df_extrato = df_extrato.query('valor_ext < 0')
        df_extrato["valor_ext"] = df_extrato["valor_ext"].abs()
        df_extrato = df_extrato.dropna(how='any')

        df_extrato = df_extrato.rename(columns={'descricao': 'custo'})
        df_extrato = df_extrato.rename(columns={'valor_ext': 'valor_custo'})
        df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
        df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_custo'})

        df_extrato = df_extrato[["tipo_custo", "custo", "valor_custo", "dt_mes_base", "dt_custo"]]
        return df_extrato


    def txt_generate_df_recebido(self, txt_local):

        print("-> gerando dataframe baseado no csv(separador ';' / decimal '.') (pasta local):", txt_local, "\n")
        
        df_extrato = pd.read_csv(txt_local, sep=';', header=None)
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext" ]

        nome_csv = txt_local.replace(".txt", ".csv")
        df_extrato.to_csv(nome_csv, index=None)
        #df_custo = pd.read_csv(txt_local.replace(".txt", ".csv"), sep=';', usecols=['dt_mes_base','dt_recebido','descricao','valor_recebido'])#, sep='\t'
        

        print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", txt_local, "\n")

        #now = dt.datetime.now()
        #dt_ = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        #print(dt_.strftime("%Y-%m-%d"))
        #teste_f = dt.datetime(year=int(dt_.strftime("%Y")), month=8, day=1)
        df_extrato["tipo_custo"] = None

        df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')

        df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%Y")
        df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%m")
        df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[0]),month=int(df_extrato['mes'].iloc[0]), day=1)
        
        #print(df_dt_base, "\n")
        #print(mes)

        #print(df_custo_tt['dt_custo_bq'].iloc[0]) 
        #print(df_custo_tt['dt_custo_bq'].iloc[-1])
        #print(pd.to_datetime(df_custo_tt['dt_custo_bq'].iloc[0]).strftime("%Y")) 
        
        #data_base = df_custo_tt['dt_custo_bq'].iloc[0]
        #print(data_base[2:6])
        #print(data_base[4:6])

        df_extrato['dt_base'] = df_dt_base 
        #pd.DatetimeIndex(pd.to_datetime(df_custo_tt['dt_custo_bq'].iloc[0]).strftime("%Y"))
        #df_custo_tt['dt_custo_bq'] = pd.to_datetime(df_custo_tt['dt_custo_bq'])
        
        print(df_extrato, "\n")
        print(df_extrato.dtypes, "\n")

        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", txt_local, "\n")
        df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(str)
        #print(df_custo["valor_custo"].dtypes, "\n")
        #print(df_custo, "\n")
        df_extrato["valor_ext"] = [x.replace(",", ".") for x in df_extrato["valor_ext"]]
        

        df_extrato["valor_ext"] = pd.to_numeric(df_extrato["valor_ext"].fillna(0), errors="coerce")
        df_extrato["valor_ext"] = df_extrato["valor_ext"].map("{:.2f}".format)
        df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(float)

        print(df_extrato, "\n")
        print(df_extrato.dtypes, "\n")

        print("-> retorna dataframe tratado", "\n")
        df_extrato = df_extrato.loc[df_extrato['valor_ext'] >= 0]
        df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')==False]
        df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'RES APLIC[^\\b]+\w')==False]
        #df_extrato = df_extrato.where(df_extrato['valor_ext'] > '0')
        print(df_extrato, "\n")
        #df_extrato = df_extrato.query('valor_ext < 0')
        #df_extrato["valor_ext"] = df_extrato["valor_ext"].abs()
        #df_extrato = df_extrato.dropna(how='any')

        #df_extrato = df_extrato.rename(columns={'descricao': 'custo'})
        df_extrato = df_extrato.rename(columns={'valor_ext': 'valor_recebido'})
        df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
        df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_recebido'})

        df_extrato = df_extrato[[ "descricao", "valor_recebido", "dt_mes_base", "dt_recebido"]]
        return df_extrato
        #valor_recebido	dt_mes_base	descricao	dt_recebido	

    def txt_generate_df_saldo(self, txt_local):

        print("-> gerando dataframe baseado no csv(separador ';' / decimal '.') (pasta local):", txt_local, "\n")
        
        df_extrato = pd.read_csv(txt_local, sep=';', header=None)
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext" ]

        nome_csv = txt_local.replace(".txt", ".csv")
        df_extrato.to_csv(nome_csv, index=None)
        #df_custo = pd.read_csv(txt_local.replace(".txt", ".csv"), sep=';', usecols=['dt_mes_base','dt_recebido','descricao','valor_recebido'])#, sep='\t'
        

        print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", txt_local, "\n")

        #now = dt.datetime.now()
        #dt_ = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        #print(dt_.strftime("%Y-%m-%d"))
        #teste_f = dt.datetime(year=int(dt_.strftime("%Y")), month=8, day=1)
        #df_extrato["tipo_custo"] = None

        df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')

        df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%Y")
        df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%m")
        df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[0]),month=int(df_extrato['mes'].iloc[0]), day=1)
        
        #print(df_dt_base, "\n")
        #print(mes)

        #print(df_custo_tt['dt_custo_bq'].iloc[0]) 
        #print(df_custo_tt['dt_custo_bq'].iloc[-1])
        #print(pd.to_datetime(df_custo_tt['dt_custo_bq'].iloc[0]).strftime("%Y")) 
        
        #data_base = df_custo_tt['dt_custo_bq'].iloc[0]
        #print(data_base[2:6])
        #print(data_base[4:6])

        df_extrato['dt_base'] = df_dt_base 
        #pd.DatetimeIndex(pd.to_datetime(df_custo_tt['dt_custo_bq'].iloc[0]).strftime("%Y"))
        #df_custo_tt['dt_custo_bq'] = pd.to_datetime(df_custo_tt['dt_custo_bq'])
        
        print(df_extrato, "\n")
        print(df_extrato.dtypes, "\n")

        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", txt_local, "\n")
        df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(str)
        #print(df_custo["valor_custo"].dtypes, "\n")
        #print(df_custo, "\n")
        df_extrato["valor_ext"] = [x.replace(",", ".") for x in df_extrato["valor_ext"]]
        

        df_extrato["valor_ext"] = pd.to_numeric(df_extrato["valor_ext"].fillna(0), errors="coerce")
        df_extrato["valor_ext"] = df_extrato["valor_ext"].map("{:.2f}".format)
        df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(float)

        print(df_extrato, "\n")

        print("-> retorna dataframe tratado", "\n")
        df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')]
        #SALDO[^\\b]+\w
        #df1[df1['col'].str.contains(r'foo(?!$)')]
        #df_extrato = df_extrato.where(df_extrato['valor_ext'] > '0')
        df_extrato["valor_ext"] = df_extrato["valor_ext"].abs()
        df_extrato = df_extrato.dropna(how='any')
        print(df_extrato, "\n")
        #df_extrato = df_extrato.query('valor_ext < 0')
        #df_extrato["valor_ext"] = df_extrato["valor_ext"].abs()
        #df_extrato = df_extrato.dropna(how='any')

        #df_extrato = df_extrato.rename(columns={'descricao': 'custo'})
        df_extrato = df_extrato.rename(columns={'valor_ext': 'saldo'})
        df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
        df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_recebido'})

        df_extrato = df_extrato[[ "descricao", "saldo", "dt_mes_base", "dt_recebido"]]
        return df_extrato

    def sheet_df_recebido(self):

        print("-> gerando dataframe baseado no sheet de previsão de recebido", "\n")
        df_recebido = pd.DataFrame(DriveSheet().leitor())#, sep='\t'
        
        print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", "\n")
        df_recebido['dt_mes_base'] = pd.to_datetime(df_recebido['dt_mes_base'],format='%d/%m/%Y')
        df_recebido['dt_recebido'] = pd.to_datetime(df_recebido['dt_recebido'],format='%d/%m/%Y')


        #print(df_custo["valor_custo"].dtypes, "\n")
        print(df_recebido.dtypes, "\n")
        print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", "\n")
        df_recebido["valor_recebido"] = df_recebido["valor_recebido"].astype(str)
        #print(df_recebido, "\n")
        #print(df_custo, "\n")
        df_recebido["valor_recebido"] = [x.replace(",", ".") for x in df_recebido["valor_recebido"]]

        df_recebido["valor_recebido"] = pd.to_numeric(df_recebido["valor_recebido"].fillna(0), errors="coerce")
        #print(df_recebido, "\n")
        df_recebido["valor_recebido"] = df_recebido["valor_recebido"].map("{:.2f}".format)
        df_recebido["valor_recebido"] = df_recebido["valor_recebido"].astype(float)
        #print(df_recebido, "\n")

        print(df_recebido.dtypes, "\n")
        #df_depara_sem_dup = df_depara.drop_duplicates()
        print("-> retorna dataframe tratado", "\n")

        return df_recebido

    def df_generate_csv():
        data = {'Product': ['Desktop Computer','Tablet','Printer','Laptop'],
        'Price': [850,200,150,1300]
        }

        df = pd.DataFrame(data, columns= ['Product', 'Price'])

        df.to_csv (r'C:\Users\Ron\Desktop\export_dataframe.csv', index = False, header=True)

        print (df)
    
    def excel_generate_df_custo(self, excel_file):

        print("GenerateDF: gerar dataframe baseado no excel:",excel_file)
        df_extrato = pd.read_excel(excel_file, sheet_name='Lançamentos', usecols = "A,B,D,E", skiprows=8) 
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext", "valor_saldo" ]
        #print(df_extrato, "\n")
        if len(df_extrato): 

            print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", excel_file, "\n")

            df_extrato["tipo_custo"] = None

            df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')

            df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%Y")
            df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[0]).strftime("%m")
            df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[0]),month=int(df_extrato['mes'].iloc[0]), day=1)

            df_extrato['dt_base'] = df_dt_base 

            print(df_extrato, "\n")
            print(df_extrato.dtypes, "\n")

            print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", excel_file, "\n")
            df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(str)

            df_extrato["valor_ext"] = [x.replace(",", ".") for x in df_extrato["valor_ext"]]
            

            df_extrato["valor_ext"] = pd.to_numeric(df_extrato["valor_ext"].fillna(0), errors="coerce")
            df_extrato["valor_ext"] = df_extrato["valor_ext"].map("{:.2f}".format)
            df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(float)

            #print(df_extrato, "\n")

            df_extrato["valor_saldo"] = df_extrato["valor_saldo"].astype(str)

            df_extrato["valor_saldo"] = [x.replace(",", ".") for x in df_extrato["valor_saldo"]]
            

            df_extrato["valor_saldo"] = pd.to_numeric(df_extrato["valor_saldo"].fillna(0), errors="coerce")
            df_extrato["valor_saldo"] = df_extrato["valor_saldo"].map("{:.2f}".format)
            df_extrato["valor_saldo"] = df_extrato["valor_saldo"].astype(float)

            print(df_extrato, "\n")

            custo_de_para = 'csv/domestico/de_para_debito.csv'
            print("-> aplicar de-para custo", custo_de_para, "\n")

            df_custo_de_para= pd.read_csv(custo_de_para, sep=';', usecols=['de_para','valor'])

            for index_cc, row_cc in df_extrato.iterrows():
                print (row_cc["descricao"], index_cc)
                idx = index_cc
                if row_cc["descricao"].find('TAR PACOTE ITAU')>=0:
                    print("primeira verificação de de-para custo => banco")
                    var_tp = 'banco'
                    df_extrato.at[idx,"tipo_custo"] = var_tp

                elif row_cc["descricao"].find('INT PAG TIT')>=0:
                    print("primeira verificação de de-para custo => boleto")
                    var_tp = 'boleto'
                    df_extrato.at[idx,"tipo_custo"] = var_tp 

                elif row_cc["descricao"].find('DA  NEXTEL TELECOM')>=0:
                    print("primeira verificação de de-para custo => celular")
                    var_tp = 'celular'
                    df_extrato.at[idx,"tipo_custo"] = var_tp    

                elif row_cc["descricao"].find('TBI')>=0:
                    print("primeira verificação de de-para custo => poupança")
                    var_tp = 'poupança'
                    df_extrato.at[idx,"tipo_custo"] = var_tp  

                elif row_cc["descricao"].find('SAQUE')>=0:
                    print("primeira verificação de de-para custo => saque")
                    var_tp = 'saque'
                    df_extrato.at[idx,"tipo_custo"] = var_tp     

                elif row_cc["descricao"].find('UNICEF')>=0:
                    print("primeira verificação de de-para custo => doação")
                    var_tp = 'doação'
                    df_extrato.at[idx,"tipo_custo"] = var_tp 

                elif ((row_cc["descricao"].find('DA  NEXTEL TELECOM') < 0)  and 
                        (row_cc["descricao"].find('INT PAG TIT') < 0)       and 
                        (row_cc["descricao"].find('TAR PACOTE ITAU') < 0)):
                    print("segunda verificação de de-para custo")
                    tipo_custo = df_custo_de_para.where(df_custo_de_para['de_para'] == row_cc["descricao"])
                    #print(tipo_custo)
                    var_tipo_custo = tipo_custo[(tipo_custo["valor"].notnull())]#.unique()
                    print(len(var_tipo_custo.index))

                    if len(var_tipo_custo.index)>0:

                        for index_dp, row_dp in var_tipo_custo.iterrows():
                            
                            if row_cc["descricao"] == row_dp['de_para']:

                                tipo_custo_x = row_dp["valor"]
                                print("for depara compara com custo => ",tipo_custo_x)
                                df_extrato.at[idx,"tipo_custo"] = tipo_custo_x
                            else:
                                df_extrato.at[idx,"tipo_custo"] = "compras"
                    else:
                        df_extrato.at[idx,"tipo_custo"] = "compras"
                else:
                    df_extrato.at[idx,"tipo_custo"] = "compras"

            print("-> retorna dataframe tratado", "\n")

            df_extrato = df_extrato.where(df_extrato['valor_ext'] < 0)
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')==False]

            df_extrato["valor_ext"] = df_extrato["valor_ext"].abs()
            df_extrato = df_extrato.dropna(how='any')

            df_extrato = df_extrato.rename(columns={'descricao': 'custo'})
            df_extrato = df_extrato.rename(columns={'valor_ext': 'valor_custo'})
            df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
            df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_custo'})

            df_extrato = df_extrato[["tipo_custo", "custo", "valor_custo", "dt_mes_base", "dt_custo"]]

        else:
            print("GenerateDF: arquivo não gerou dataframe de dados", "\n")

        print(df_extrato, "\n")
        return df_extrato

    def pandas_teste(self):
        records = [
            {
                "tipo_custo": u"tipo_custo",
                "custo": u"custo",
                "valor_custo": 0,
                "dt_pagto_bq": '27/05/2001',
                "dt_custo_bq": '27/05/2001',
            },
            {
                "tipo_custo": u"tipo_custo",
                "custo": u"custo",
                "valor_custo": "112,5",
                "dt_pagto_bq": '27/05/2001',
                "dt_custo_bq": '27/05/2001',
            },
            {
                "tipo_custo": u"tipo_custo",
                "custo": u"custo",
                "valor_custo": "112,55",
                "dt_pagto_bq": '27/05/2001',
                "dt_custo_bq": '27/05/2001',
            },
        ]
        dataframe = pd.DataFrame(
            records,
            # In the loaded table, the column order reflects the order of the
            # columns in the DataFrame.
            columns=[
                "tipo_custo",
                "custo",
                "valor_custo",
                "dt_pagto_bq",
                "dt_custo_bq"
            ],
            # Optionally, set a named index, which can also be written to the
            # BigQuery table.
            index=pd.Index(
                [u"aaa",u"bbbb",u"ccc"], name="tipo_custo"
            ),
        )
        now = dt.datetime.now()

        dt_ = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))
        print(dt_) # 2015-10-08 06:30:22.348341+09:00
        # se quiser mostrar em outro formato
        #print(dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")) # 08/10/2015 06:30:22.348341 +0900

        #dataframe['process_time'] = dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")
        #print(dataframe)
        dataframe['dt_pagto_bq'] = pd.to_datetime(dataframe['dt_pagto_bq'],format='%d/%m/%Y')
        dataframe['dt_custo_bq'] = pd.to_datetime(dataframe['dt_custo_bq'],format='%d/%m/%Y')

        dataframe["valor_custo"] = dataframe["valor_custo"].astype(str)
        dataframe["valor_custo"] = [x.replace(",", ".") for x in dataframe["valor_custo"]]
        dataframe["valor_custo"] = pd.to_numeric(dataframe["valor_custo"].fillna(0), errors="coerce")
        dataframe["valor_custo"] = dataframe["valor_custo"].map("{:.2f}".format)
        dataframe["valor_custo"] = dataframe["valor_custo"].astype(float)

        print(dataframe.dtypes, "\n")
        return dataframe

