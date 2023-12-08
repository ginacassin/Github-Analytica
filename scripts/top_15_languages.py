from script_interface import ScriptInterface
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType


class Top15Languages(ScriptInterface):
    def __init__(self):
        super().__init__('Top 15 languages')

    def get_15_top_languages(self):
        """
        Works mainly with the 'languages' table.
        Gets the top 15 languages used in the repositories, this is done thanks to the language_name column.
        :return: Spark dataframe with the top 15 languages.
        """
        # Obtain 'languages' table
        languages_df = self.get_table('languages')

        # Convert the 'language_name' column to an array of strings
        languages_df = languages_df.withColumn('language_name', f.from_json('language_name', ArrayType(StringType())))

        # Explode the 'language_name' array into separate rows, using repo_name as id, with alias language
        exploded_df = languages_df.select('repo_name', f.explode('language_name').alias('language'))

        # Group by language name and count occurrences, with alias count
        language_counts = exploded_df.groupBy('language').count().alias('count')

        # Order by count in descending order
        sorted_languages = language_counts.orderBy('count', ascending=False)

        # Select the top 15 languages
        top_languages = sorted_languages.limit(15)

        return top_languages

    def process_data(self):
        # Obtain the top 15 languages
        top_15_languages_df = self.get_15_top_languages()

        # Log and print the result (if in test mode)
        if self.test_mode:
            top_15_languages_df.show(truncate=False)
            self.log.info('Top 15 languages: \n %s', top_15_languages_df.toPandas())

        # Save the result to a CSV file
        self.save_data(top_15_languages_df, 'top_15_languages')


if __name__ == "__main__":
    top_15_languages = Top15Languages()
    top_15_languages.run()
