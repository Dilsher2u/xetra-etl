"""
Connector and methods accessing S3
"""
import logging
import os
import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from xetra.common.constants import S3FileTypes
from .custom_exceptions import WrongFileFormatException

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
        self.endpoint_url = endpoint_url    # type: str
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
    
    def read_csv_to_df(self, key:str, encoding:str = 'utf-8', sep:str = ','):
        """
        Read a CSV file from S3 bucket and return a pandas dataframe
        
        :param key: key of the file to read
        :param encoding: encoding of the file
        :param sep: separator of the file
        
        return: 
            df: pandas dataframe
        """
        self._logger.info('Reading file from S3 bucket: {}'.format(key))
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, df: pd.DataFrame, key: str, file_format: str):

        if df.empty:
            self._logger.info('Empty dataframe, nothing to write')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)

        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)

        self._logger.info('The file format %s is not '
        'supported to be written to s3!', file_format)
        raise WrongFileFormatException("No file found")


    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()
        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
