from google.cloud import storage

class MovimentacaoFileBucket():

    def pull_file(self, bckt, dir, file):
        # create storage client
        storage_client = storage.Client.from_service_account_json('resource/key.json')
        # get bucket with name
        bucket = storage_client.get_bucket(bckt)
        # get bucket data as blob
        # proj = projeto
        blob = bucket.get_blob(projeto+dir+file)
        # convert to string
        bcontent = blob.download_as_string()
        return bcontent.decode("utf8")

    def upload_blob(bckt, dir, dir_dest, file):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # proj = 'devsamelo'
        storage_client = storage.Client.from_service_account_json('resource/key.json')
        bucket = storage_client.bucket(bckt)
        
        source_file_name = dir+file
        destination_blob_name = dir_dest+file
        
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )

    def getfiles(self):
        # create storage client
        storage_client = storage.Client.from_service_account_json('resource/key.json')
        # get bucket with name
        bucket = storage_client.get_bucket('dev-custo-csv')
        # get bucket data as blob
        blob = bucket.get_blob('extrator/csv_host_teste.csv')
        # convert to string
        bcontent = blob.download_as_string()
        return bcontent.decode("utf8")

    def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
        """Copies a blob from one bucket to another with a new name."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"
        # destination_bucket_name = "destination-bucket-name"
        # destination_blob_name = "destination-object-name"

        storage_client = storage.Client().from_service_account_json('resource/key.json')

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

    def delete_blob(bucket_name, blob_name):
        """Deletes a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"

        storage_client = storage.Client().from_service_account_json('resource/key.json')

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print("Blob {} deleted.".format(blob_name))
