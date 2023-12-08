from script_interfaceee import ScriptInterface
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

class nReposReadme(ScriptInterface):
    def __init__(self):
        super().__init__('Number of repos with README')

    
    def get_number_of_repos(self):
        """
        We will use files table. Counting the number of times we find a 
        README.md file in path column
        """

        #obtain files table
        files_df = self.get_table('files')

        #create new column with value 1 if path contains string "README.md"
        contains_readme = F.when(files_df['path'].like('%README.md%'), 1).otherwise(0)

        #add contains_readme column into files_df 
        files_df = files_df.withColumn('contains_readme', contains_readme)

        #count how many of them contain "README.md" file
        readme_count = files_df.groupBy('repo_name').agg(F.sum('contains_readme').alias('readme_count'))

        return readme_count





    def process_data(self):
        #Obtain number of repos containing readme file
        number_repos_Readme = self.get_number_of_repos()

        #log and print the result
        if self.test_mode:
            number_repos_Readme.show(truncate = False)
            count_value = number_repos_Readme.agg(F.sum('readme_count')).collect()[0][0]
            self.log.info('Number of repos that contain file Readme.md: %d', count_value)
        
        #save result to CSV file
        self.save_data(number_repos_Readme, 'number_repos_readme')



if __name__ == "__main__":
    countReadMe = nReposReadme()
    countReadMe.run()





