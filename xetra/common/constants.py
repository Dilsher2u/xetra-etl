"""
File to store constants
"""

from enum import Enum

class S3FIleTypes(Enum):
    """
    Enum to store S3 file types
    """
    CSV = 'csv'
    JSON = 'json'
    PARQUET = 'parquet'

class MetaProcessFormat(Enum):
    """
    Enum to store meta process format
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COLUMN = 'source_date'
    META_PROCESS_COLUMN = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
    
