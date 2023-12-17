from script_interface import ScriptInterface
from pyspark.sql import functions as f

class ReposMarkdown(ScriptInterface):
    def __init__(self):
        super().__init__('Number of repos with markdown file different from "README.md"')

    def get_number_of_repos(self):
        """
        Works mainly with the 'files' table.
        Gets how many repos work with a Markdown file different from "README.md".
        :return: Spark dataframe with the amount of repos with a Markdown file different from "README.md".
        """
        files_df = self.get_table('files')

        # Check if 'path' contains '.md' but not 'README.md'
        contains_markdown = f.when((f.col('path').like('%.md%')) & (~f.col('path').contains('README.md')), 1).otherwise(0)

        files_df = files_df.withColumn('contains_markdown', contains_markdown)

        # Count one if the repository use a different .md than README.md
        result_df = files_df.groupBy('repo_name').agg(
            f.sum('contains_markdown').alias('repos_contains_markdown')
        )

        # Count how many repositories in total use another Markdown file
        markdown_df = result_df.groupBy().agg(
            f.sum('repos_contains_markdown').alias('repos_contains_markdown_different_from_readme')
        )

        return markdown_df

    def process_data(self):
        repos_with_markdown_df = self.get_number_of_repos()

        if self.test_mode:
            self.log.info("Quantity of repos with a different .md than README.md:")
            repos_with_markdown_df.show(truncate=False)

        # Save repos with markdown DataFrame to a CSV file
        self.save_data(repos_with_markdown_df, 'repos_with_markdown')


if __name__ == "__main__":
    countMarkdown = ReposMarkdown()
    countMarkdown.run()
