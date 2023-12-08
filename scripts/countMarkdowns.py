from script_interfaceee import ScriptInterface
from pyspark.sql import functions as F

class nReposMarkdown(ScriptInterface):
    def __init__(self):
        super().__init__('Number of repos with markdown file different from "README.md')

    
    def get_number_of_repos(self):
        """
        We will use files table. Counting the number of times we find a 
        markdown file with different name than README.md file in path column
        """

        #obtain files table
        files_df = self.get_table('files')

        #create new column with value 1 if path contains string ".md" file different from "README.md"
        #HAY QUE CAMBIAR ALGO EN ESTA LINEA
        contains_markdown = F.when((files_df['path'].like('%.md%')) and (files_df['path'] != 'README.md'), 1).otherwise(0)

        #add contains_markdown column into files_df 
        files_df = files_df.withColumn('contains_markdown', contains_markdown)

        #count how many of them contain "README.md" file
        markdown_count = files_df.groupBy('repo_name').agg(F.sum('contains_markdown').alias('markdown_count'))

        return markdown_count


    def process_data(self):
        #Obtain number of repos containing readme file
        number_repos_markdown= self.get_number_of_repos()

        #log and print the result
        if self.test_mode:
            number_repos_markdown.show(truncate = False)
            count_value = number_repos_markdown.agg(F.sum('markdown_count')).collect()[0][0]
            self.log.info('Number of repos that contain  markdown file different from Readme.md: %d', count_value)
        
        #save result to CSV file
        self.save_data(number_repos_markdown, 'number_repos_markdown')



if __name__ == "__main__":
    countMarkdown = nReposMarkdown()
    countMarkdown.run()





