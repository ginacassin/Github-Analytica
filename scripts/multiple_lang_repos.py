import argparse
from script_interface import ScriptInterface
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations


class MultiLanguages(ScriptInterface):
    def __init__(self):
        super().__init__('Most used language combinations in repositories by a specific language', language=True)
        self.language_filter = self.parser.parse_args().language
        self.log.info('Filtering by language: %s', self.language_filter)

    def multi_languages(self):
        """
        Works with the 'languages' table.
        Obtain the combination of languages by input language, ordered by count in descending order.
        :return: 3 Spark dataframe with multi-language statistics.
        """
        # Obtain 'languages' table
        languages_df = self.get_table('languages')

        # Convert the 'language_name' column to an array of strings
        filtered_languages_df = languages_df.withColumn('language_name',
                                                        f.from_json('language_name', ArrayType(StringType())))

        # Filter only repositories with more than one language
        multi_language_repos_df = filtered_languages_df.filter(f.size('language_name') > 1)

        # Count the number of repositories with more than one language
        total_multi_language_repos = multi_language_repos_df.count()

        # Calculate the average number of languages per repository
        avg_languages_per_repo = \
            filtered_languages_df.agg(f.avg(f.size('language_name')).alias('avg_languages')).collect()[0][
                'avg_languages']

        # Calculate the combinations of languages used in repositories
        language_combinations = multi_language_repos_df.filter(f.array_contains('language_name', self.language_filter))

        # Count occurrences of each combination
        language_combination_counts = language_combinations.groupBy('language_name').count()

        # Order by count in descending order. Show only the top 25 combinations
        sorted_combinations = language_combination_counts.orderBy('count', ascending=False).limit(25)

        return total_multi_language_repos, avg_languages_per_repo, sorted_combinations

    def process_data(self):
        # Obtain multi-language statistics
        total_multi_language_repos, avg_languages_per_repo, sorted_combinations = self.multi_languages()

        # Create a DataFrame with the results
        stats_df = self.spark.createDataFrame([(total_multi_language_repos, avg_languages_per_repo)],
                                              ["total_multi_language_repos", "avg_languages_per_repo"])

        # Log and print the combined result (if in test mode)
        if self.test_mode:
            stats_df.show(truncate=False)
            sorted_combinations.show(n=25, truncate=False)
            self.log.info('Stats: \n%s', stats_df.toPandas())
            self.log.info('Language combinations for %s: \n%s', self.language_filter, sorted_combinations.toPandas())

        # Save the combined result to a CSV file
        self.save_data(stats_df, 'mul_lang_stats')
        self.save_data(sorted_combinations, 'mul_lang_combinations_' + self.language_filter)


if __name__ == "__main__":
    multi_language_repos = MultiLanguages()
    multi_language_repos.run()
