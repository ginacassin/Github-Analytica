![image](https://github.com/ginacassin/Github-Analytica/assets/63422931/3489a7cb-ab03-4c54-8f33-24c2e64d2eb2)

# Github-Analytica
## Introduction
GitHub, the go-to hub for developers worldwide, has seen a whopping 12 million contributors shaping around 31 million projects since 2008. It's the busy epicenter of open-source development.

Now, picture this: you're a student navigating the coding universe or someone just stepping into a new stack. GitHub, with its countless projects, feels like a vast library without a roadmap. The challenge here is real - finding your way through the code maze.

So, what's the plan? We're diving into the GitHub realm to decode the secrets. What programming languages are dominating the scene and what tricks are being used.

Why bother, you ask? For students grasping coding nuances or those embarking on a new tech stack, it's like having a guidebook. We aim to demystify GitHub, making it more navigable, and bring you insights into the coding practices that define different languages.  Ready to jump into the GitHub adventure with us? 🚀

### Need of Big Data
Since we’ll be working with huge amounts of data, we need a powerful tool that allows us to perform all the required operations and analysis. 
The large dataset considered for this project comprises more than 2.8 million open source GitHub repositories, over 145 million unique commits, 2 billion different file paths, and the contents of the latest revision for 163 million files. 

## Run requirements
- Python 3.9
- Pip. Install the following dependencies:
  - Python-dotenv: `pip install python-dotenv`
  - Pandas: `pip install pandas`
- [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- Set .env file with Bucket path to the datasets (see .env for reference)

## Architecture
- 

## Project structure

The project consists of 9 scripts, each with a specific purpose. They are built from an interface in order to streamline the coding process.

The scripts, and a brief explanation of what they do, are as follows:


- **[Top 15 languages](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/top_15_languages.py)**: obtains the top 15 languages used in GitHub repositories. 
- **[Top 5 licenses](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/top_5_licenses.py)**: obtains the top 5 open-source licenses used in GitHub repositories.
- **[Main vs master](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/main_vs_master.py)**: obtains the number of repositories that use the master branch vs the main branch as a head branch.
- **[Most active repos](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/top_repos_active.py)**: obtains the 25 repos with the most commits and at least one commit in the last two years.
- **[How many repos have READMEs](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/count_read_me.py)**: obtains how many repos have a README as documentation.
- **[How many repos have .md](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/count_markdowns_not_readme.py)**: obtains how many repos have a file.md but isn't a README.
- **[Top 5 single language repositories](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/single_language_repos.py)**: obtains the top 5 languages used in the repositories with just one language.
- **[Multiple language repositories](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/multiple_lang_repos.py)**: obtains and combines multi-language statistics for repositories. Needs argument -l or --language and a language. Includes: 
  - Total count of repositories with more than one language
  - The average number of languages per repository
  - The top 25 combinations of languages of a certain language. For example, the top 25 languages combinations used in repositories that use Python.
- **[Top build tools](https://github.com/ginacassin/Github-Analytica/blob/main/scripts/top_build_tools.py)**: obtains the top build tools used with the number of repositories using them.


## How to run
### Local
1. Clone the repository.
2. Install the requirements.
3. Run the script you want to execute:
   1. There's a flag for testing purposes (`-t` or `--test`), which will run the script on a small subset of the data. For example, to run the `top_15_languages` script on a small subset of the data, run `spark-submit scripts/top_15_languages.py -t`.
   2. Some scripts have a flag (`-l` or `--language`) to choose the language to filter the script. For example, to run the `top_5_single_language_repositories` script for the language Python, run `spark-submit scripts/top_5_single_language_repositories.py -t -l Python`.
   3. The logs of the script can be found in the `logs` folder. These provide a cleaner view of the script's execution.

### GCP
1. Clone the scripts in the bucket, in the same directories as in the repo. Also store the datasets in `/data`
2. Install requirements:
  ```
  python -m pip install python-dotenv
  
  export BUCKET={bucket_dir}
  
  cd ~
  mkdir logs
  
  touch logs/logs.log
  ```

3. Scripts can be run with the following commands: `spark-submit --py-files $BUCKET/scripts/script_interface.py $BUCKET/scripts/{script_name}.py`. For example, to run the `top_15_languages` script, run `spark-submit --py-files $BUCKET/scripts/script_interface.py $BUCKET/scripts/top_15_languages.py`. 

## About the datasets
When searching for large datasets, we found out Google Cloud offers multiple public datasets through the BigQuery platform. Our dataset originates directly from GitHub, publicly available on BigQuery under "[Github Activity Data on BigQuery](https://console.cloud.google.com/marketplace/product/github/github-repos)". It has data up to November of 2022.
The dataset contains information from open source GitHub repositories, and from each one of them it provides very detailed information about commits, contents, files, languages and licenses.  
The dataset exceeds 3TB in size, in total. It consists of the following tables: commits, contents, files, languages, licenses, sample_commits, sample_contents, sample_files and sample_repos. 

For our scripts, we’ll be focusing mainly on `languages`, `licenses`, `sample_contents`, `sample_files` and `sample_commits` which come up to 37GB in size.

### Test dataset
The test dataset is a small subset of the original dataset. It is used for testing purposes and is not included in the final results.
The datasets were obtained using the following query on BigQuery and then downloaded as a .csv:

``SELECT * FROM `bigquery-public-data.github_repos. {table_name}` LIMIT 1000``

These can be found on the `resources` folder.

Some of these datasets had to be cleaned in order to be used, mostly because in BigQuery some values were stored as records (so when it downloaded, it downloaded as a column with a .json file). The cleaning scripts used can be found in the `src` folder. This was mostly useful for the scripts that used the table `languages`.

### BigQuery datasets
Due to size and limitations on costs in GCP, the datasets used were the samples ones. For example, the table `contents` weighs 2.44TB while `sample_contents` just 24GB.

Nonetheless, the scripts provided in this repo are able to run with these huge datasets. 

The aforementioned datasets were downloaded from BigQuery and uploaded to GCP, to streamline the process of developing and executing the scripts and costs.
