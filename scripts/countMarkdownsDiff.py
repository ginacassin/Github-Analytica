from script_interface import ScriptInterface
from pyspark.sql import functions as F

class nReposMarkdown(ScriptInterface):
    def __init__(self):
        super().__init__('Number of repos with markdown file different from "README.md"')

    def get_number_of_repos(self):
        files_df = self.get_table('files')

        # Check if 'path' contains '.md' but not 'README.md'
        contains_markdown = F.when((F.col('path').like('%.md%')) & (~F.col('path').contains('README.md')), 1).otherwise(0)

        files_df = files_df.withColumn('contains_markdown', contains_markdown)

        # Filter repositories with a different .md than README.md
        repos_with_markdown_df = files_df.filter(F.col('contains_markdown') == 1).select('repo_name').distinct()

        return repos_with_markdown_df

    def process_data(self):
        repos_with_markdown_df = self.get_number_of_repos()

        if self.test_mode:
            print("Repos with a different .md than README.md:")
            repos_with_markdown_df.show(truncate=False)

            # Calculate and display total count
            total_repos_with_markdown = repos_with_markdown_df.count()

            print(f"Total repos with a different .md than README.md: {total_repos_with_markdown}")

        # Save repos with markdown DataFrame to a CSV file
        self.save_data(repos_with_markdown_df, 'repos_with_markdown')


if __name__ == "__main__":
    countMarkdown = nReposMarkdown()
    countMarkdown.run()
