"""
Runninng the application
"""
import logging
import logging.config
import yaml

def main():
    """
    entry point to run ETL job
    """
    #Parsing yaml file
    config_path = 'C:/Users/dilsh/OneDrive/Desktop/projects/ETL-Python/xetra-etl/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    
    #configuring logging
    log_config = config['logging']

    #load config as dict
    logging.config.dictConfig(log_config)

    logger = logging.getLogger(__name__)
    logger.info('Starting ETL job')

if __name__ == '__main__':
    main()

