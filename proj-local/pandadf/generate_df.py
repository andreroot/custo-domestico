import pandas as pd
from bucket.mov_file_bucket import MovimentacaoFileBucket
import datetime as dt

class GenerateDF():
   

    def csv_generate_df_depara_debito(self, csv_local):

        print("-> gerando dataframe baseado no csv (pasta local):", csv_local, "\n")
        df_depara = pd.read_csv(csv_local, sep=';',usecols=['de_para','valor'])#, sep='\t'

        print("-> remove duplicadas", "\n")
        df_depara_sem_dup = df_depara.drop_duplicates()
        
        return df_depara_sem_dup


    def csv_generate_df_custo(self, csv_local):

        print("-> gerando dataframe baseado no csv(separador ';' / decimal '.') (pasta local):", csv_local, "\n")
        df_custo = pd.read_csv(csv_local, sep=';', decimal=".", usecols=['tipo_custo','custo','valor_custo','dt_pagto_bq','dt_custo_bq'])#, sep='\t'
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
        #df_custo["valor_custo"] = [x.replace(",", ".") for x in df_custo["valor_custo"]]

        df_custo["valor_custo"] = pd.to_numeric(df_custo["valor_custo"].fillna(0), errors="coerce")
        df_custo["valor_custo"] = df_custo["valor_custo"].map("{:.2f}".format)
        df_custo["valor_custo"] = df_custo["valor_custo"].astype(float)

        print(df_custo.dtypes, "\n")
        #df_depara_sem_dup = df_depara.drop_duplicates()
        print("-> retorna dataframe tratado", "\n")

        return df_custo
  
    def df_generate_csv():
        data = {'Product': ['Desktop Computer','Tablet','Printer','Laptop'],
        'Price': [850,200,150,1300]
        }

        df = pd.DataFrame(data, columns= ['Product', 'Price'])

        df.to_csv (r'C:\Users\Ron\Desktop\export_dataframe.csv', index = False, header=True)

        print (df)
    


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

