import argparse
import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


class ScriptInterface:
    """
    Script interface.
    Creates a Spark session according to the given app name and obtains the datasets according to the .env file.
    :argument app_name: Name of the Spark application.
    :type app_name: str
    """

    def __init__(self, app_name, language=None):
        self.parser = self.set_parser(language)

        load_dotenv()

        self.app_name = app_name
        self.log = self.set_logging()  # Set logging configuration
        self.spark = self.create_spark_session()
        self.test_mode = self.parser.parse_args().test
        self.data_set = 'bigquery-public-data.github_repos'

        self.log.info('Test mode: %s', self.test_mode)

    def set_parser(self, language=None):
        # Set parsing arguments
        parser = argparse.ArgumentParser()
        if language is True: # If the script needs to be filtered by language
            parser.add_argument('-l', '--language', type=str, help='Choose language to filter', required=True)
        parser.add_argument('-t', '--test', action='store_true', help='Set to launch in test mode')

        return parser

    def create_spark_session(self):
        """
        Creates a Spark session.
        :return: Spark session.
        """
        # Create Spark configuration and context
        conf = SparkConf().setAppName(self.app_name)
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')

        # Create Spark session
        return SparkSession(sc)

    def set_logging(self):
        """
        Configures the logging tool. The level is INFO and the format is: [app_name] [timestamp] [level] [message].
        Logs will be stored in logs folder.
        :return: Logger object.
        """
        # Create logger
        logger = logging.getLogger(self.app_name)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('[%(name)s] %(asctime)s %(levelname)s %(message)s') # Create formatter

        # Create file handler and set formatter
        file_handler = logging.FileHandler('./logs/logs.log')
        file_handler.setFormatter(formatter)

        # Add file handler to logger
        logger.addHandler(file_handler)

        return logger

    def get_table(self, table_name):
        """
        Gets the table according to the test_mode variable set by the test_mode argument.
        :param table_name: Name of the table to load.
        :type table_name: str
        :return: Spark dataframe with the table data.
        """
        if self.test_mode:
            return self.load_data_test(table_name)
        else:
            return self.load_data_prod(table_name)

    def load_data_test(self, table_name):
        """
        Loads the table determined by table name for test mode. These are located on resources' folder in .csv format.
        :param table_name: Name of the table to load.
        :type table_name: str
        :return: Spark dataframe with the table data.
        """
        self.log.info('Loading table %s for test mode', table_name)
        file_path = f'./resources/{table_name}.csv'

        return self.spark.read.option("nullValue", "").csv(file_path, header=True, inferSchema=True)

    def load_data_prod(self, table_name):
        """
        Loads the table determined by table name for production mode. These are located on a GCP Bucket.
        :param table_name: Name of the table to load.
        :type table_name: str
        :return: Spark dataframe with the table data.
        """
        self.log.info('Loading table %s for production mode', table_name)
        bucket = os.getenv('BUCKET')

        return self.spark.read.option("nullValue", "").csv(f'{bucket}/{table_name}.csv', header=True, inferSchema=True)

    def process_data(self):
        """
        Process the data. Implement this method in the script child class.
        """
        raise NotImplementedError('This method should be implemented in the child class')

    def save_data(self, df, table_name):
        """
        Saves the dataframe to a CSV file.
        :param df: Dataframe to save.
        :type df: pyspark.sql.dataframe.DataFrame
        :param table_name: Name of the table to save.
        :type table_name: str
        """
        self.log.info('Saving result %s.csv', table_name)

        df.toPandas().to_csv(f'./results/{table_name}.csv', index=False, sep=',')

    def run(self):
        """
        Runs the script. This method should not be modified.
        """
        self.log.info('Running script %s', self.__class__.__name__)
        self.process_data()
        self.log.info('Script %s finished', self.__class__.__name__)