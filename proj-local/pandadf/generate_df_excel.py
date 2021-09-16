import pandas as pd
from bucket.mov_file_bucket import MovimentacaoFileBucket
from drive.sheet import DriveSheet
import numpy as np
import datetime as dt
import warnings
import openpyxl

class GenerateDF():

    def aplicar_depara_credito(self, df_extrato):

        custo_de_para = 'csv/domestico/de_para_credito.csv'
        print("-> aplicar de-para custo", custo_de_para, "\n")

        df_custo_de_para= pd.read_csv(custo_de_para, sep=';', usecols=['de_para','valor'])

        for index_cc, row_cc in df_extrato.iterrows():
            print (row_cc["descricao"], index_cc)
            idx = index_cc
            var_tp = None
            if row_cc["descricao"].find('Ph5 Fitness')>=0:
                print("primeira verificação de de-para custo => banco")
                var_tp = 'academia'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Sonda')>=0:
                print("primeira verificação de de-para custo => boleto")
                var_tp = 'mercado'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp 

            elif row_cc["descricao"].find('Trimais')>=0:
                print("primeira verificação de de-para custo => celular")
                var_tp = 'mercado'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp    

            elif row_cc["descricao"].find('Ebanx*spotify')>=0:
                print("primeira verificação de de-para custo => poupança")
                var_tp = 'spotify'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp  

            elif row_cc["descricao"].find('The Walt Disney Company')>=0:
                print("primeira verificação de de-para custo => saque")
                var_tp = 'disney'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp     

            elif row_cc["descricao"].find('Netflix.com')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'netflix'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp 

            elif row_cc["descricao"].find('Uber')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'uber'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp 

            elif row_cc["descricao"].find('Zoom.us')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'zoom'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp 

            elif row_cc["descricao"].find('Microsoft*microsoft 365')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'ondrive'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp 

            elif row_cc["descricao"].find('Amazonprimebr')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'amazon'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Dl     *google Google')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'google'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Conectcar   *conectcar')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'conectar'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Parcelamen Fatura')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'cartão_parcelado'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Localiza')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'aluguel_carro'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Ph5 Fitness')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'academia'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp

            elif row_cc["descricao"].find('Posto')>=0:
                print("primeira verificação de de-para custo => doação")
                var_tp = 'carro'
                df_extrato.at[idx,"tipo_custo_credito"] = var_tp
            
            else:
                print("Não encontrado")
                var_tp = "compras"

            if var_tp=="compras":

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
                            df_extrato.at[idx,"tipo_custo_credito"] = tipo_custo_x
                        else:
                            df_extrato.at[idx,"tipo_custo_credito"] = "compras"
                else:
                    df_extrato.at[idx,"tipo_custo_credito"] = "compras"
        
        return df_extrato

    def aplicar_depara(self, df_extrato):

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

            elif row_cc["descricao"].find('LIS/JUROS')>=0:
                print("primeira verificação de de-para custo => banco")
                var_tp = 'banco'
                df_extrato.at[idx,"tipo_custo"] = var_tp 

            else:
                print("Não encontrado")
                var_tp = "compras"

            if var_tp=="compras":

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
        
        return df_extrato

    def excel_generate_df_custo(self, excel_file):

        print("GenerateDF - custo: gerar dataframe baseado no excel:",excel_file)
        df_extrato = pd.read_excel(excel_file, sheet_name='Lançamentos', usecols = "A,B,D,E", skiprows=8) 
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext", "valor_saldo" ]
        #print(df_extrato, "\n")
        if len(df_extrato): 

            print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", excel_file, "\n")

            df_extrato["tipo_custo"] = None

            df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')
            #-> regra para gerar data base do mes, usar "loc" e pegar uma posição dos dados para gerar a data
            df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%Y")
            df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%m")
            df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[1]),month=int(df_extrato['mes'].iloc[1]), day=1)

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

            df_extrato = GenerateDF().aplicar_depara(df_extrato)

            #print(df_extrato, "\n")

            print("-> retorna dataframe tratado", "\n")

            df_extrato = df_extrato.where(df_extrato['valor_ext'] < 0)
            #print(df_extrato, "\n")
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')==False]
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'APL[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False]
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'RES[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False]
            #print(df_extrato, "\n")
            df_extrato["valor_ext"] = df_extrato["valor_ext"].abs()
            #print(df_extrato, "\n")
            #df_extrato = df_extrato.dropna(how='any')
            #print(df_extrato, "\n")

            df_extrato = df_extrato.rename(columns={'descricao': 'custo'})
            df_extrato = df_extrato.rename(columns={'valor_ext': 'valor_custo'})
            df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
            df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_custo'})

            print("-> inserir data_process", "\n")
            
            now = dt.datetime.now()

            dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

            df_extrato['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
            df_extrato = df_extrato[["tipo_custo", "custo", "valor_custo", "dt_mes_base", "dt_custo","process_time"]]

        else:
            print("GenerateDF: arquivo não gerou dataframe de dados", "\n")

        print(df_extrato, "\n")
        return df_extrato

    def excel_generate_df_credito(self, excel_file):

        print("GenerateDF - faura de credito: gerar dataframe baseado no excel:",excel_file)
        df_extrato = pd.read_excel(excel_file, sheet_name='Lançamentos', usecols = "A,B,D,E", skiprows=1) 
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext","dt_base"]
        #print(df_extrato, "\n")
        if len(df_extrato): 

            print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", excel_file, "\n")

            df_extrato["tipo_custo_credito"] = None

            df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')
            df_extrato['dt_base'] = pd.to_datetime(df_extrato['dt_base'],format='%d/%m/%Y')
            #-> regra para gerar data base do mes, usar "loc" e pegar uma posição dos dados para gerar a data
            #df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%Y")
            #df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%m")
            
            #df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[1]),month=int(df_extrato['mes'].iloc[1]), day=1)

            #df_extrato['dt_base'] = df_dt_base 

            print(df_extrato, "\n")
            print(df_extrato.dtypes, "\n")

            print("-> convert colunas de valor(com separador ',' para '.') no dataframe (str to float)", excel_file, "\n")
            df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(str)

            df_extrato["valor_ext"] = [x.replace(",", ".") for x in df_extrato["valor_ext"]]
            

            df_extrato["valor_ext"] = pd.to_numeric(df_extrato["valor_ext"].fillna(0), errors="coerce")
            df_extrato["valor_ext"] = df_extrato["valor_ext"].map("{:.2f}".format)
            df_extrato["valor_ext"] = df_extrato["valor_ext"].astype(float)

            print(df_extrato, "\n")

            df_extrato = GenerateDF().aplicar_depara_credito(df_extrato)

            #print(df_extrato, "\n")

            print("-> retorna dataframe tratado", "\n")

            df_extrato = df_extrato.rename(columns={'descricao': 'custo_credito'})
            df_extrato = df_extrato.rename(columns={'valor_ext': 'valor_credito'})
            df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
            df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_credito'})

            print("-> inserir data_process", "\n")
            
            now = dt.datetime.now()

            dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

            df_extrato['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
            df_extrato = df_extrato[["tipo_custo_credito", "custo_credito", "valor_credito", "dt_mes_base", "dt_credito","process_time"]]

        else:
            print("GenerateDF: arquivo não gerou dataframe de dados", "\n")

        print(df_extrato, "\n")
        return df_extrato

    def excel_generate_df_recebido(self, excel_file):

        print("GenerateDF - recebido: gerar dataframe baseado no excel:",excel_file)
        #necessario tratar os excel os recebimentos do mes base, aparece o mes seguinte, causado o desbalanceamento dos saldos
        df_extrato = pd.read_excel(excel_file, sheet_name='Lançamentos', usecols = "A,B,D,E", skiprows=8) 
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext", "valor_saldo" ]
        #print(df_extrato, "\n")
        if len(df_extrato): 

            print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", excel_file, "\n")

            df_extrato["tipo_custo"] = None

            df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')
            #-> regra para gerar data base do mes, usar "loc" e pegar uma posição dos dados para gerar a data
            df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%Y")
            df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%m")
            df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[1]),month=int(df_extrato['mes'].iloc[1]), day=1)

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

            df_extrato = GenerateDF().aplicar_depara(df_extrato)

            #print(df_extrato, "\n")

            print("-> retorna dataframe tratado", "\n")

            df_extrato = df_extrato.loc[df_extrato['valor_ext'] >= 0]
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')==False]
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'RES APLIC[^\\b]+\w')==False]

            df_extrato = df_extrato.rename(columns={'valor_ext': 'valor_recebido'})
            df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
            df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_recebido'})

            print("-> inserir data_process", "\n")
            
            now = dt.datetime.now()

            dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

            df_extrato['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

            df_extrato = df_extrato[[ "descricao", "valor_recebido", "dt_mes_base", "dt_recebido", "process_time"]]

        else:
            print("GenerateDF: arquivo não gerou dataframe de dados", "\n")

        print(df_extrato, "\n")
        return df_extrato

    def excel_generate_df_saldo(self, excel_file):

        print("GenerateDF - saldo: gerar dataframe baseado no excel:",excel_file)
        df_extrato = pd.read_excel(excel_file, sheet_name='Lançamentos', usecols = "A,B,D,E", skiprows=8) 
        df_extrato.columns = ["dt_extrato_bq", "descricao", "valor_ext", "valor_saldo" ]
        #print(df_extrato, "\n")
        if len(df_extrato): 

            print("-> convert colunas de data(formato YYYY-MM-DD) no dataframe (str to date)", excel_file, "\n")

            df_extrato["tipo_custo"] = None

            df_extrato['dt_extrato_bq'] = pd.to_datetime(df_extrato['dt_extrato_bq'],format='%d/%m/%Y')
            #-> regra para gerar data base do mes, usar "loc" e pegar uma posição dos dados para gerar a data
            df_extrato['ano'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%Y")
            df_extrato['mes'] = pd.to_datetime(df_extrato['dt_extrato_bq'].iloc[1]).strftime("%m")
            df_dt_base = dt.datetime(year=int(df_extrato['ano'].iloc[1]),month=int(df_extrato['mes'].iloc[1]), day=1)

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

            print("-> retorna dataframe tratado", "\n")

            #df_extrato = df_extrato.loc[df_extrato['valor_saldo'] >= 0]
            df_extrato = df_extrato[df_extrato['descricao'].str.contains(r'SALDO[^\\b]+\w')]
            
            #df_extrato["valor_saldo"] = df_extrato["valor_saldo"].abs()
            #df_extrato = df_extrato.dropna(how='any')
            print(df_extrato, "\n")
 
            df_extrato = df_extrato.rename(columns={'valor_saldo': 'saldo'})
            df_extrato = df_extrato.rename(columns={'dt_base': 'dt_mes_base'})
            df_extrato = df_extrato.rename(columns={'dt_extrato_bq': 'dt_recebido'})
            print("-> inserir data_process", "\n")
            
            now = dt.datetime.now()

            dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

            df_extrato['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

            df_extrato = df_extrato[[ "descricao", "saldo", "dt_mes_base", "dt_recebido", "process_time"]]

        else:
            print("GenerateDF: arquivo não gerou dataframe de dados", "\n")

        print(df_extrato, "\n")
        return df_extrato
