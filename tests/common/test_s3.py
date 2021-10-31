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

       

if __name__ == '__main__':
    unittest.main()