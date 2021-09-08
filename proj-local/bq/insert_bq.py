from google.cloud import bigquery
import google.auth
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from google.oauth2 import service_account

class InsertBq():

    def insert_bucket_tabela(self, dataset, tabela, bckt, dir, file_in):
        # Construct a BigQuery client object.
        #client = bigquery.Client()
        client = bigquery.Client.from_service_account_json('resource/key.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = 'devsamelo2'+'.'+dataset+'.'+tabela

        # Set the encryption key to use for the destination.
        # TODO: Replace this key with a key you have created in KMS.
        # kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
        #     "cloud-samples-tests", "us", "test", "test"
        # )
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            field_delimiter=";",
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        #uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
        uri = "gs://"+bckt+dir+file_in
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))


    def insert_df_depara(self, df, dataset, file_in):

        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('resource/key.json')
        dataset_ref = client.dataset(dataset, project="devsamelo2")
        table_ref = dataset_ref.table(file_in)
        
        # TODO(developer): Set table_id to the ID of table to append to.
        table_id = 'devsamelo2.'+dataset+'.'+file_in

        # Create a new table.
        schema = [
            {'name': 'de_para', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'valor', 'type': 'STRING', 'mode': 'nullable'}
        ]
        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.
            bigquery.SchemaField("de_para", bigquery.enums.SqlTypeNames.STRING),#{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("valor", bigquery.enums.SqlTypeNames.STRING)
        ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def insert_df_custo(self, df, dataset, file_in):
        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('resource/key.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = 'devsamelo2.'+dataset+'.'+file_in
        schema = [
            {'name': 'tipo_custo', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'custo', 'type': 'STRING', 'mode': 'nullable'},            
            {'name': 'valor_custo', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'dt_mes_base', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'dt_custo', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'process_time', 'type': 'TIMESTAMP', 'mode': 'nullable'}   
        ]
        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.
            bigquery.SchemaField("tipo_custo", bigquery.enums.SqlTypeNames.STRING),#{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("custo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("valor_custo", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("dt_custo", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP)
        ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            #write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def insert_df_saldo(self, df, dataset, file_in):
        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('resource/key.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = 'devsamelo2.'+dataset+'.'+file_in
        #'dt_mes_base','descricao','valor_recebido','dt_recebido'
        # na mesma posição que esta no dataframe
        schema = [
         
            {'name': 'descricao', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'saldo', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'dt_mes_base', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'dt_recebido', 'type': 'DATE', 'mode': 'nullable'} ,
            {'name': 'process_time', 'type': 'TIMESTAMP', 'mode': 'nullable'}      
            #{'name': 'valor_previsto', 'type': 'FLOAT', 'mode': 'nullable'}
            ]
        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.

            bigquery.SchemaField("descricao", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("saldo", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
            #{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("dt_recebido", bigquery.enums.SqlTypeNames.DATE),           
            bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP) 
        ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            #write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def insert_df_recebido(self, df, dataset, file_in):
        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('resource/key.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = 'devsamelo2.'+dataset+'.'+file_in
        #'dt_mes_base','descricao','valor_recebido','dt_recebido'
        # na mesma posição que esta no dataframe
        schema = [
         
            {'name': 'descricao', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'valor_recebido', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'dt_mes_base', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'dt_recebido', 'type': 'DATE', 'mode': 'nullable'} , 
            {'name': 'process_time', 'type': 'TIMESTAMP', 'mode': 'nullable'}   
            #{'name': 'valor_previsto', 'type': 'FLOAT', 'mode': 'nullable'}
            ]
        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.

            bigquery.SchemaField("descricao", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("valor_recebido", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
            #{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("dt_recebido", bigquery.enums.SqlTypeNames.DATE),      
            bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP) 

        ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            #write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )


    def insert_df_credito(self, df, dataset, file_in):
        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('resource/key.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = 'devsamelo2.'+dataset+'.'+file_in
        #'dt_mes_base','descricao','valor_recebido','dt_recebido'
        # na mesma posição que esta no dataframe
        schema = [
            {'name': 'tipo_custo_credito', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'custo_credito', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'valor_credito', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'dt_mes_base', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'dt_credito', 'type': 'DATE', 'mode': 'nullable'} , 
            {'name': 'process_time', 'type': 'TIMESTAMP', 'mode': 'nullable'}   
            #{'name': 'valor_previsto', 'type': 'FLOAT', 'mode': 'nullable'}
            ]
        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.
            bigquery.SchemaField("tipo_custo_credito", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("custo_credito", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("valor_credito", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
            #{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("dt_credito", bigquery.enums.SqlTypeNames.DATE),      
            bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP) 

        ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            #write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def create_job(self, query):
        '''
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        '''
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/bigquery"]
        #credentials = service_account.Credentials.from_service_account_file('resource/key.json')
        #scoped_credentials = credentials.with_scopes(scopes)
        # Create credentials with Drive & BigQuery API scopes.
        # Both APIs must be enabled for your project before running this code.
        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )

        # Construct a BigQuery client object.
        client = bigquery.Client(credentials=credentials, project='devsamelo2')

        #client =  bigquery.Client('devsamelo2', credentials)

        #client = bigquery.Client.from_service_account_json('resource/key.json')

        query_job = client.query(
            query,
            # Explicitly force job execution to be routed to a specific processing
            # location.
            location="US",
            # Specify a job configuration to set optional job resource properties.
            job_config=bigquery.QueryJobConfig(
                labels={"analise": "custo_domestico"}
            ),
            # The client libraries automatically generate a job ID. Override the
            # generated ID with either the job_id_prefix or job_id parameters.
            job_id_prefix="custo_domestico_",
        )  # Make an API request.

        print("Started job: {}".format(query_job.job_id))
        # [END bigquery_create_job]
        return query_job        


    #gerar tabela sheet:https://docs.google.com/spreadsheets/d/165LxRPISVoidWCXTPw7cCKnMAOO0p0zzNvJshET_5Qk/edit?resourcekey#gid=2013346928
    #CUSTO
    #data_base_bq:DATE,custo:STRING,tipo_custo:STRING,dt_custo_bq:DATE,valor_custo:FLOAT,ano_base:INTEGER,mes_base_ordem:INTEGER,mes_base:STRING