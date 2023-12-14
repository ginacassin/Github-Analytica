#spark-submit scripts/top_file_names.py -t

from script_interface import ScriptInterface
from pyspark.sql.functions import split, count, col,element_at 
import sys

class TopFileNames(ScriptInterface):
    def __init__(self):
        super().__init__('Top file names')

    def common_file_names(self):
        """
        Retrieves the top 5 most common file names of the specified extension and their counts from the 'files' table.

        Returns:
            PySpark DataFrame: DataFrame containing the top 5 common file names and their counts.
        """

        # Obtain 'files' table
        files_df = self.get_table('files')

        # Get the file name from the "path" column.
        df_transformed = files_df.withColumn("path", element_at(split(col("path"), "/"),-1))
        df_transformed = df_transformed.withColumn("extension", split(col("path"), "\.")[1])
        df_go_files = df_transformed.filter(col("extension") == "py")

        # Group by file name and count.
        df_result = df_go_files.groupBy("path").agg(count("*").alias("count"))

        # Order by desc.
        df_result = df_result.orderBy(col("count").desc()).limit(5)


        return df_result

    def process_data(self):
        top_file_names = self.common_file_names()

        # Log and print the result (if in test mode)
        if self.test_mode:
            top_file_names.show(truncate=False)
            self.log.info('Top file names: \n %s', top_file_names.toPandas())

        # Save the result to a CSV file
        self.save_data(top_file_names, 'top_file_names')


if __name__ == "__main__":
    top_5_licenses = TopFileNames()
    top_5_licenses.run()