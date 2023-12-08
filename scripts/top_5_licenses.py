from script_interface import ScriptInterface


class Top5Licenses(ScriptInterface):
    def __init__(self):
        super().__init__('Top 5 licenses')

    def get_top_5_licenses(self):
        """
        Works mainly with the 'licenses' table.
        Gets the top 5 open source licenses used in the repositories, this is done thanks to the license column.
        :return: Spark dataframe with the top 5 licenses.
        """
        # Obtain 'licenses' table
        licenses_df = self.get_table('licenses')

        # Group by license name and count occurrences, with alias count
        licenses_count = licenses_df.groupBy('license').count().alias('count')

        # Order by count in descending order
        sorted_licenses = licenses_count.orderBy('count', ascending=False)

        # Select the top 5 licenses
        top_licenses = sorted_licenses.limit(5)

        return top_licenses

    def process_data(self):
        # Obtain the top 5 licenses
        top_5_licenses_df = self.get_top_5_licenses()

        # Log and print the result (if in test mode)
        if self.test_mode:
            top_5_licenses_df.show(truncate=False)
            self.log.info('Top 5 licenses: \n %s', top_5_licenses_df.toPandas())

        # Save the result to a CSV file
        self.save_data(top_5_licenses_df, 'top_5_licenses')


if __name__ == "__main__":
    top_5_licenses = Top5Licenses()
    top_5_licenses.run()
