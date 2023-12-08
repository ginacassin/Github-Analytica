from script_interface import ScriptInterface
from pyspark.sql import functions as f


class MainVsMaster(ScriptInterface):
    def __init__(self):
        super().__init__('Main vs Master head branch naming')

    def get_count_main_master(self):
        """
        Works mainly with the 'files' table. Counts how many repositories still work with the 'master' branch instead
        of the 'main' branch, and how many repositories have already changed to the 'main' branch (or they currently
        use this naming).
        :return: Spark dataframe with the count of repositories that use 'master' or 'main'
        naming convention branch.
        """
        # Obtain 'files' table
        files_df = self.get_table('files')
        # Work only with the columns repo_name and ref
        files_df = files_df.select('repo_name', 'ref')

        # Filter only repos with ref master or main
        filtered_repos = files_df.filter((f.col('ref').isin('refs/heads/master', 'refs/heads/main')))

        # Group by repo_name and count if ref is master or main
        result_df = filtered_repos.groupBy('repo_name').agg(
            f.countDistinct(f.when(f.col('ref') == 'refs/heads/main', 'main')).alias('main_count'),
            f.countDistinct(f.when(f.col('ref') == 'refs/heads/master', 'master')).alias('master_count')
        )

        # Count how many repositories use 'master' or 'main' branch
        main_master_df = result_df.groupBy().agg(
            f.sum('main_count').alias('main'),
            f.sum('master_count').alias('master')
        )

        return main_master_df

    def process_data(self):
        # Obtain the count of repositories that use 'master', 'main' or other naming convention branch
        main_master_df = self.get_count_main_master()

        # Log and print the result (if in test mode)
        if self.test_mode:
            main_master_df.show(truncate=False)
            self.log.info('Main, master and others head branch naming count: \n %s', main_master_df.toPandas())

        # Save the result to a CSV file
        self.save_data(main_master_df, 'main_vs_master')


if __name__ == "__main__":
    main_vs_master = MainVsMaster()
    main_vs_master.run()
