"""
Connector and methods accessing S3
"""
import logging
import os
import boto3


class S3BucketConnector():
    """
    Class for interacting with S# buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector
        
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param endpoint_url: AWS endpoint URL
        :param bucket: S3 bucket name

        """

        
        self._logger = logging.getLogger(__name__)
        self.access_key = access_key        # type: str
        self.secret_key = secret_key        # type: str
        self.endpoint_url = endpoint_url    # type: str
        self.bucket = bucket                # type: str

        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name = 's3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    
    def list_files_in_prefix(self, prefix: str):

        """
        List files with a prefix on S3 bucket
        :param prefix: prefix to list files that needs to be filtered

        return: 
            files: list of files with the prefix
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
    
    def read_csv_to_df(self, file_name: str):
        pass

    def write_df_to_s3(self):
        pass
