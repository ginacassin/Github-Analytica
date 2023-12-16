from script_interface import ScriptInterface
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType

class SingleLanguages(ScriptInterface):
    def __init__(self):
        super().__init__('Top 5 single language repositories')

    def single_language_repos(self):
        """
        Works with the 'languages' table.
        Gets the top 5 languages used in the repositories with just 1 language, this is done thanks to the language_name column.
        :return: Spark dataframe with the top 5 single repositories languages and their counts.
        """
        # Obtain 'languages' table
        languages_df = self.get_table('languages')

        # Convert the 'language_name' column to an array of strings
        languages_df = languages_df.withColumn('language_name', f.from_json('language_name', ArrayType(StringType())))

        # Filter repositories with only one language
        single_language_repos_df = languages_df.filter(f.size('language_name') == 1)

        # Group by language name and count occurrences, with alias count
        language_counts = single_language_repos_df.groupBy('language_name').count().alias('count')

        # Order by count in descending order
        sorted_languages = language_counts.orderBy('count', ascending=False)

        # Select the top 5 languages
        top_languages = sorted_languages.limit(5)

        return top_languages

    def process_data(self):
        # Obtain the top 5 languages
        top_5_single_languages_df = self.single_language_repos()

        # Log and print the result (if in test mode)
        if self.test_mode:
            top_5_single_languages_df.show(truncate=False)
            self.log.info('Top 5 single languages: \n %s', top_5_single_languages_df.toPandas())

        # Save the result to a CSV file
        self.save_data(top_5_single_languages_df, 'top_5_single_languages')


if __name__ == "__main__":
    single_language_repos = SingleLanguages()
    single_language_repos.run()
