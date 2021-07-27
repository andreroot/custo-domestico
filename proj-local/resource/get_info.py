from bucket.mov_file_bucket import MovimentacaoFileBucket
import pandas as pd

class GetInfo():

    def csv_df_teste():
        data = {'Product': ['Desktop Computer','Tablet','Printer','Laptop'],
        'Price': [850,200,150,1300]
        }

        df = pd.DataFrame(data, columns= ['Product', 'Price'])

        df.to_csv (r'C:\Users\Ron\Desktop\export_dataframe.csv', index = False, header=True)

        print (df)
    
    def df_csv_teste(self, file_in):

        file_dp_in = file_in+'.csv'    
        #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
        MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'csv/domestico/', 'de-para/', file_dp_in)
        df_depara = pd.read_csv ('csv/domestico/'+file_dp_in, sep=';',usecols= ['de_para','valor'])#, sep='\t'
        
        return df_depara

        #--> definir arquivo que sera gerado no storage para depois converter em tabela
        #file_custo_in = "custo_2021.csv"

        #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
        #MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'csv/domestico/', 'extrator/', file_in)
        #df_custo = pd.read_csv ('csv/domestico/'+file_custo_in, sep=';',usecols= ['descrição','lançamento','valor_convertido_saida (R$)','dt_pagto_bq','dt_custo_bq'])#, sep='\t'
        
        #"descrição";"data";"lançamento";"ag./origem";"valor (R$)";"valor_convertido_saida (R$)";"valor_convertido_entrada (R$)";"REF";"dt_pagto_bq";"dt_custo_bq"
        #print(df_custo)

    def dataframe_pedido(self, query):
        #db_connection_str = 'mysql+pymysql://bio:8Xaje&0FFA@pegue-prod.csjvwyz3iltv.us-east-1.rds.amazonaws.com/db_pegue'
        #db_connection = create_engine(db_connection_str)
        
        db = ConectMysql.conn()
        # Executa a consulta na tabela selecionada
        # MovimentacaoFileBucket().upload_blob(query)

        df = pd.read_sql(MovimentacaoFileBucket().getfilessql(query), con=db)
        now = datetime.datetime.now()

        dt = datetime.datetime.fromtimestamp(datetime.datetime.timestamp(now))
        print(dt) # 2015-10-08 06:30:22.348341+09:00
        # se quiser mostrar em outro formato
        #print(type(dt.strftime("%Y-%m-%d %H:%M:%S.%f %z"))) # 08/10/2015 06:30:22.348341 +0900
        
        df['process_time'] = dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")
        df['process_time'] = pd.to_datetime(df['process_time'])
        df['code'] = df['code'].astype(str)
        df['customer_id'] = df['customer_id'].astype(str)
        
        #print(df.dtypes)
        pd.set_option('display.expand_frame_repr', False)
        #print(df.head())

        return df

    def pandas_teste(self):
        records = [
            {
                "title": u"The Meaning of Life",
                "release_year": 1983,
                "length_minutes": 112.5,
                "release_date": pytz.timezone("Europe/Paris")
                .localize(datetime.datetime(1983, 5, 9, 13, 0, 0))
                .astimezone(pytz.utc),
                # Assume UTC timezone when a datetime object contains no timezone.
                "dvd_release": datetime.datetime(2002, 1, 22, 7, 0, 0),
            },
            {
                "title": u"Monty Python and the Holy Grail",
                "release_year": 1975,
                "length_minutes": 91.5,
                "release_date": pytz.timezone("Europe/London")
                .localize(datetime.datetime(1975, 4, 9, 23, 59, 2))
                .astimezone(pytz.utc),
                "dvd_release": datetime.datetime(2002, 7, 16, 9, 0, 0),
            },
            {
                "title": u"Life of Brian",
                "release_year": 1979,
                "length_minutes": 94.25,
                "release_date": pytz.timezone("America/New_York")
                .localize(datetime.datetime(1979, 8, 17, 23, 59, 5))
                .astimezone(pytz.utc),
                "dvd_release": datetime.datetime(2008, 1, 14, 8, 0, 0),
            },
            {
                "title": u"And Now for Something Completely Different",
                "release_year": 1971,
                "length_minutes": 88.0,
                "release_date": pytz.timezone("Europe/London")
                .localize(datetime.datetime(1971, 9, 28, 23, 59, 7))
                .astimezone(pytz.utc),
                "dvd_release": datetime.datetime(2003, 10, 22, 10, 0, 0),
            },
        ]
        dataframe = pd.DataFrame(
            records,
            # In the loaded table, the column order reflects the order of the
            # columns in the DataFrame.
            columns=[
                "title",
                "release_year",
                "length_minutes",
                "release_date",
                "dvd_release",
            ],
            # Optionally, set a named index, which can also be written to the
            # BigQuery table.
            index=pd.Index(
                [u"Q24980", u"Q25043", u"Q24953", u"Q16403"], name="wikidata_id"
            ),
        )
        now = datetime.datetime.now()

        dt = datetime.datetime.fromtimestamp(datetime.datetime.timestamp(now))
        print(dt) # 2015-10-08 06:30:22.348341+09:00
        # se quiser mostrar em outro formato
        print(dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")) # 08/10/2015 06:30:22.348341 +0900

        dataframe['process_time'] = dt.strftime("%Y-%m-%d %H:%M:%S.%f %z")
        print(dataframe)

    def get_depara_deb():

        file_in = 'de-para-debito.csv'    
        #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
        MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'csv/domestico/', 'de-para/', file_in)
        


        dataset = 'dev_domestico'
        tabela_bq = base
        #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
        InsertBq.insert_tabela( dataset, tabela_bq, 'proj-domestico-file/', 'extrator/', file_in)