import os
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


class ScriptInterface:
    """ Script model class.
    Creates a Spark session according to the given app name and obtains the datasets according to the .env file.
    :argument app_name: Name of the Spark application.
    :type app_name: str
    """

    def __init__(self, app_name):
        self.spark = self.create_spark_session(app_name)
        self.test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        self.data_set = os.getenv('DATA_SET')

    @staticmethod
    def create_spark_session(app_name):
        # Create Spark configuration and context
        conf = SparkConf().setAppName(app_name)
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        # Create Spark session
        return SparkSession(sc)

    @staticmethod
    def set_logging():
        """
        Configures the logging tool. The level is INFO and the format is: [app_name] [timestamp] [level] [message].
        Logs will be stored in logs folder.
        """
        logging.basicConfig(format='[%(name)s] %(asctime)s %(levelname)s %(message)s', level=logging.INFO)
        logging.getLogger().addHandler(logging.FileHandler('logs/logs.log'))

    def get_table(self, table_name):
        """
        Gets the table according to the test_mode variable set by the .env file.
        """
        if self.test_mode:
            return self.load_data_test(table_name)
        else:
            return self.load_data_prod(table_name)

    def load_data_test(self, table_name):
        """
        Loads the table determined by table name for test mode. These are located on resources' folder in .csv format.
        """
        logging.info('Loading table %s for test mode', table_name)
        file_path = f'resources/{table_name}.csv'

        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def load_data_prod(self, table_name):
        """
        Loads the table determined by table name for production mode. These are located on a public BigQuery dataset.
        """
        logging.info('Loading table %s for production mode', table_name)

        return self.spark.read.format("bigquery").option("table", f"{self.data_set}.{table_name}").load()
