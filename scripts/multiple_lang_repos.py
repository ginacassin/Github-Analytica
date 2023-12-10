from script_interface import ScriptInterface
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations

class MultiLanguages(ScriptInterface):
    def __init__(self):
        super().__init__('Top 5 multi-language repositories')

    def multi_language_stats(self):
        """
        Works with the 'languages' table.
        Gets statistics on repositories with more than one language.
        :return: Spark dataframe with multi-language statistics.
        """
        # Obtain 'languages' table
        languages_df = self.get_table('languages')

        # Convert the 'language_name' column to an array of strings
        languages_df = languages_df.withColumn('language_name', f.from_json('language_name', ArrayType(StringType())))

        # Check if the array has more than one language, filter repositories with more than one language
        multi_language_repos_df = languages_df.filter(f.size('language_name') > 1)

        # Count the number of repositories with more than one language
        total_multi_language_repos = multi_language_repos_df.count()

        # Calculate the average number of languages per repository
        avg_languages_per_repo = languages_df.agg(f.avg(f.size('language_name')).alias('avg_languages')).collect()[0]['avg_languages']

        # Calculate the combinations of languages used in repositories
        language_combinations = multi_language_repos_df.withColumn('language_combinations', f.udf(lambda langs: list(combinations(sorted(langs), 2)), ArrayType(ArrayType(StringType())))('language_name'))

        # Flatten the combinations and count occurrences
        language_combination_counts = language_combinations.select(f.explode('language_combinations').alias('language_combination')).groupBy('language_combination').count()

        # Order by count in descending order
        sorted_combinations = language_combination_counts.orderBy('count', ascending=False)

        # Select the top 5 language combinations
        top_combinations = sorted_combinations.limit(5)

        return total_multi_language_repos, avg_languages_per_repo, top_combinations

    def process_data(self):
        # Obtain multi-language statistics
        total_multi_language_repos, avg_languages_per_repo, top_combinations = self.multi_language_stats()

        # Log and print the results (if in test mode)
        if self.test_mode:
            self.log.info('Total repositories with more than one language: %d', total_multi_language_repos)
            self.log.info('Average number of languages per repository: %.2f', avg_languages_per_repo)
            top_combinations.show(truncate=False)
            self.log.info('Top 5 language combinations: \n%s', top_combinations.toPandas())

        # Save the results to CSV files
        self.save_data(total_multi_language_repos, 'total_multi_language_repos')
        self.save_data(avg_languages_per_repo, 'avg_languages_per_repo')
        self.save_data(top_combinations, 'top5_combinations_multi_lang')


if __name__ == "__main__":
    multi_language_repos = MultiLanguages()
    multi_language_repos.run()
