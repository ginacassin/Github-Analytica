from script_interface import ScriptInterface
from pyspark.sql.functions import col  # Import col function

class TabsVsSpaces(ScriptInterface):
    def __init__(self):
        super().__init__('Top 5 licenses')

    def tabs_vs_spaces(self):
        """
        """
        # Obtain 'files' table
        files_df = self.get_table('files')

        # Obtain 'contents' table
        contents_df = self.get_table('contents')

        # Filtrar los archivos por extensión
        filtered_files_df = files_df.filter(col("path").rlike(r'\.([^\.]*)$'))

        # Realizar la unión de los marcos de datos
        result_df = filtered_files_df.join(
            contents_df,
            filtered_files_df["id"] == contents_df["id"],
            how="inner"
        ).select(
            filtered_files_df["id"],
            # filtered_files_df["size"],
            # contents_df["content"],
            # filtered_files_df["binary"],
            # filtered_files_df["copies"],
            # filtered_files_df["sample_repo_name"],
            # filtered_files_df["path"]
        )


        return result_df

    def process_data(self):
        top_5_licenses_df = self.tabs_vs_spaces()

        # Log and print the result (if in test mode)
        if self.test_mode:
            top_5_licenses_df.show(truncate=False)
            self.log.info('Top 5 licenses: \n %s', top_5_licenses_df.toPandas())

        # Save the result to a CSV file
        self.save_data(top_5_licenses_df, 'top_5_licenses')


if __name__ == "__main__":
    top_5_licenses = TabsVsSpaces()
    top_5_licenses.run()
