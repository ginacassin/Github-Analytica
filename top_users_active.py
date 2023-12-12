from script_interface import ScriptInterface
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime, timedelta

class MostActiveRepos(ScriptInterface):
    def __init__(self):
        super().__init__('Most Active Users in Repositories')

    def get_most_active_repositories(self):
        """
        Identify the most active users with at least one commit in the last year in the repository.
        :return: Spark dataframe with the most active users in repositories.
        """
        # Obtain 'commits' table
        commits_df = self.get_table('commits')

        # Group by author_name and count the number of commits, with alias commit_count
        commits_df = commits_df.filter(commits_df['author_name'].isNotNull())
        repo_commit_count = commits_df.groupBy('author_name').count()

        # Filter repositories with at least one commit in the last year
        active_repositories = repo_commit_count.filter(repo_commit_count['count'] > 0)

        # Order by count in descending order
        sorted_repos = active_repositories.orderBy('count', ascending=False)

        return sorted_repos

    def process_data(self):
        # Obtain the most active repositories
        most_active_repos_df = self.get_most_active_repositories()

        # Log the count of active repositories
        self.log.info('Count of Active Repositories: %d', most_active_repos_df.count())

        # Save the result to a CSV file
        self.save_data(most_active_repos_df, 'most_active_users_in_repos')

        # Show the result (if in test mode)
        if self.test_mode:
            most_active_repos_df.show(truncate=False)
            self.log.info('Most Active Users in Repositories: \n %s', most_active_repos_df.toPandas())


if __name__ == "__main__":
    most_active_repos = MostActiveRepos()
    most_active_repos.run()
