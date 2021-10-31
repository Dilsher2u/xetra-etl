""""
Test S3BucketConnector methods
"""

import os
import unittest

import boto3
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethod(unittest.TestCase):
    """
    Test S3BucketConnector methods
    """

    def setUp(self):
        """
        Setup the environment for the tests
        """
        # mocking s3 connection
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        #Defining class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-east-1.amazonaws.com'
        self.s3_bucket_name = 'xetra-test-dilsher'

        # Creating S3 access keys as environment variables
        os.environ[self.s3_access_key] = 'key1'
        os.environ[self.s3_secret_key] = 'key2'

        # Creating S3 bucket
        self.s3_client = boto3.resource('s3',
                                      endpoint_url=self.s3_endpoint_url)

        self.s3_client.create_bucket(Bucket=self.s3_bucket_name)
        self.s3_bucket = self.s3_client.Bucket(self.s3_bucket_name)

        #Creating a testing instance
        self.s3_bucket_connector = S3BucketConnector(
                                                     self.s3_access_key,
                                                     self.s3_secret_key,
                                                     self.s3_endpoint_url,
                                                     self.s3_bucket_name)

    def tearDown(self):
        """
        Cleanup the environment after the tests
        """
        # Mocking S3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Test the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket

        """
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # test init
        csv_conent = """col1, col2
        valA, valA
        """
        self.s3_bucket.put_object(Key=key1_exp, Body=csv_conent)
        self.s3_bucket.put_object(Key=key2_exp, Body=csv_conent)

        # Method execution
        list_result = self.s3_bucket_connector.list_files_in_prefix(prefix_exp)

        #Test after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        # Cleanup
        self.s3_bucket.delete_objects(Delete={'Objects': [{'Key': key1_exp}, {'Key': key2_exp}]})   


    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Test the list_files_in_prefix method in case of wrong prefix

        """
        prefix_exp = 'no-prefix/'
       
        # Method execution
        list_result = self.s3_bucket_connector.list_files_in_prefix(prefix_exp)

        #Test after method execution
        self.assertTrue(not list_result)

    def test_read_csv_to_df_ok(self):
        """
        Test the read_csv_to_df method for getting a dataframe
        with 2 rows and 2 columns

        """
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = 'Reading file from S3 bucket: {}'.format(key_exp)

        # test init
        csv_conent = f'{col1_exp}, {col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Key=key_exp, Body=csv_conent)

        # Method execution
        with self.assertLogs(level='INFO') as log:
            df_result = self.s3_bucket_connector.read_csv_to_df(key_exp)

            # Log test after method execution
            self.assertIn(log_exp, log.output[0])
        
        # Test after method execution
        self.assertEqual(df_result.shape[0],1)
        self.assertEqual(df_result.shape[1],2)
        self.assertEqual(val1_exp, df_result.iloc[0][col1_exp])
        self.assertEqual(val2_exp, df_result.iloc[0][col2_exp])

        # cleanup
        self.s3_bucket.delete_objects(Delete={'Objects': [{'Key': key_exp}]})

if __name__ == '__main__':
    unittest.main()