from script_interface import ScriptInterface
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType

class SingleLanguages(ScriptInterface):
    def __init__(self):
        super().__init__('Top 5 multiple language repositories')

    def multi_language_repos(self):
        """
        Works with the 'languages' table.
        Gets the count of repositories with more than one language, the average count, and the most common combination.
        :return: Tuple with count of repositories, average count, and the most common combination.
        """
        # Obtain 'languages' table
        languages_df = self.get_table('languages')

        # Convert the 'language_name' column to an array of strings
        languages_df = languages_df.withColumn('language_name', f.from_json('language_name', ArrayType(StringType())))

        # Filter repositories with more than one language
        multi_language_repos_df = languages_df.filter(f.size('language_name') > 1)

        # Count the number of repositories
        count_repositories = multi_language_repos_df.select(f.countDistinct('repository_id')).first()[0]

        # Calculate the average count
        avg_count = multi_language_repos_df.groupBy('repository_id').count().agg(f.avg('count')).first()[0]

        # Find the most common combination
        most_common_combination = multi_language_repos_df.groupBy('language_name').count().orderBy('count', ascending=False).limit(1).first()

        return count_repositories, avg_count, most_common_combination

    def process_data(self):
        # Obtain multi-language repositories statistics
        count_repositories, avg_count, most_common_combination = self.multi_language_repos()

        # Log and print the result (if in test mode)
        if self.test_mode:
            self.log.info('Number of repositories with more than one language: %d', count_repositories)
            self.log.info('Average count of languages per repository: %f', avg_count)
            self.log.info('Most common combination: %s, Count: %d', most_common_combination['language_name'], most_common_combination['count'])

        # Save the result to a CSV file (if needed)
        # self.save_data(result_df, 'result_filename')


if __name__ == "__main__":
    multi_language_repos = SingleLanguages()
    multi_language_repos.run()
