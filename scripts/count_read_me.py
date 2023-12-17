from script_interface import ScriptInterface
from pyspark.sql import functions as f

class ReposReadme(ScriptInterface):
    def __init__(self):
        super().__init__('Number of repos with README')

    def get_number_of_repos(self):
        """
        Works mainly with the 'files' table.
        Gets how many repos work with README.md and how many don't.
        :return: Spark dataframe with the amount of repos with README.md and the repos without README.md.
        """
        files_df = self.get_table('files')

        # Check if 'path' ends with 'README.md'
        contains_readme = f.when(files_df['path'].like('%README.md%'), 1).otherwise(0)
        files_df = files_df.withColumn('contains_readme', contains_readme)

        # Group by repo_name and count if README.md is present or not
        result_df = files_df.groupBy('repo_name').agg(
            f.sum('contains_readme').alias('repos_with_readme'),
            f.sum(f.when(f.col('contains_readme') == 0, 1).otherwise(0)).alias('repos_without_readme')
        )

        # Count how many repositories use 'README' or not
        readme_df = result_df.groupBy().agg(
            f.sum('repos_with_readme').alias('repos_with_readme'),
            f.sum('repos_without_readme').alias('repos_without_readme')
        )

        return readme_df

    def process_data(self):
        readme_df = self.get_number_of_repos()

        if self.test_mode:
            self.log.info("Repos with README/without README:")
            readme_df.show(truncate=False)

        # Save DataFrames to CSV files
        self.save_data(readme_df, 'repos_readme')

if __name__ == "__main__":
    countReadMe = ReposReadme()
    countReadMe.run()