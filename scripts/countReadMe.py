from script_interface import ScriptInterface
from pyspark.sql import functions as F

class nReposReadme(ScriptInterface):
    def __init__(self):
        super().__init__('Number of repos with README')

    def get_number_of_repos(self):
        files_df = self.get_table('files')

        # Check if 'path' ends with 'README.md'
        contains_readme = F.when(files_df['path'].like('%README.md%'), 1).otherwise(0)
        contains_readme = F.when(files_df['symlink_target'].like('%README.md%'), 1).otherwise(0)


        files_df = files_df.withColumn('contains_readme', contains_readme)
        
        # Count distinct repositories with 'README.md'
        repos_with_readme_df = files_df.filter(F.col('contains_readme') == 1).select('repo_name').distinct()
        
        # Count distinct repositories without 'README.md'
        repos_without_readme_df = files_df.filter(F.col('contains_readme') == 0).select('repo_name').distinct()
        
        return repos_with_readme_df, repos_without_readme_df

    def process_data(self):
        repos_with_readme_df, repos_without_readme_df = self.get_number_of_repos()

        if self.test_mode:
            print("Repos with README:")
            repos_with_readme_df.show(truncate=False)

            print("Repos without README:")
            repos_without_readme_df.show(truncate=False)

            # Calculate and display total counts
            total_repos_with_readme = repos_with_readme_df.count()
            total_repos_without_readme = repos_without_readme_df.count()

            print(f"Total Repos with README: {total_repos_with_readme}")
            print(f"Total Repos without README: {total_repos_without_readme}")

        # Save DataFrames to CSV files
        self.save_data(repos_with_readme_df, 'repos_with_readme')
        self.save_data(repos_without_readme_df, 'repos_without_readme')

if __name__ == "__main__":
    countReadMe = nReposReadme()
    countReadMe.run()