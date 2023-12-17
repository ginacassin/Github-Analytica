from script_interface import ScriptInterface
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime, timedelta

class MostActiveRepos(ScriptInterface):
    def __init__(self):
        super().__init__('Most active repos')

    def get_most_active_repos(self):
        """
        Identify the most active users with at least one commit in the last two years in the repository.
        :return: Spark dataframe with the most active users in repositories, how many commits they have and
        the repositories they have contributed to.
        """
        # Obtain 'commits' table
        commits_df = self.get_table('commits').select('date_seconds', 'repo_name')
        # Drop rows with null values in 'date_seconds'
        commits_df = commits_df.na.drop(subset=['date_seconds'])

        # Filter repositories with at least one commit in the last two years
        two_years_ago = int((datetime.now() - timedelta(days=365 * 2)).timestamp())
        active_repos = commits_df.filter(commits_df['date_seconds'] > two_years_ago)

        # Obtain the most active repositories (top 25)
        repo_commit_count = active_repos.groupBy('repo_name').count().limit(25)

        # Order by count in descending order
        sorted_repos = repo_commit_count.orderBy('count', ascending=False)

        return sorted_repos

    def process_data(self):
        # Obtain the most active repositories
        most_active_repos_df = self.get_most_active_repos()

        # Save the result to a CSV file
        self.save_data(most_active_repos_df, 'most_active_repos')

        # Show the result (if in test mode)
        if self.test_mode:
            most_active_repos_df.show(truncate=False)
            self.log.info('Most active repos: \n %s', most_active_repos_df.toPandas())


if __name__ == "__main__":
    most_active_repos = MostActiveRepos()
    most_active_repos.run()
