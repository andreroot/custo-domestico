from google.cloud import bigquery


class InsertBq():

    def insert_tabela(dataset, tabela, file_in):
        # Construct a BigQuery client object.
        #client = bigquery.Client()
        client = bigquery.Client.from_service_account_json('resource/key.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = 'devsamelo'+'.'+dataset+'.'+tabela

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
        uri = "gs://dev-custo-csv/extrator/"+file_in
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))