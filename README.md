# Github-Analytica
## Introduction
GitHub, the go-to hub for developers worldwide, has seen a whopping 12 million contributors shaping around 31 million projects since 2008. It's the busy epicenter of open-source development.

Now, picture this: you're a student navigating the coding universe or someone just stepping into a new stack. GitHub, with its countless projects, feels like a vast library without a roadmap. The challenge here is real - finding your way through the code maze.

So, what's the plan? We're diving into the GitHub realm to decode the secrets. What programming languages are dominating the scene and what trusty sidekicks (dependencies) are being used.

Why bother, you ask? For students grasping coding nuances or those embarking on a new tech stack, it's like having a guidebook. We aim to demystify GitHub, making it more navigable, and bring you insights into the coding practices that define different languages.  Ready to jump into the GitHub adventure with us? 🚀

## Run requirements
- Python 3.9
- Pip. Install the following dependencies:
  - Python-dotenv: `pip install python-dotenv`
  - Pandas: `pip install pandas`
- [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- Set .env file with Bucket path to the datasets (see .env for reference)

## Architecture


## Project structure

The project consists of x number of scripts, each with a specific purpose. They are built from an interface in order to streamline the coding process.

The scripts, and a brief explanation of what they do, are as follows:

- **Top 15 languages:** obtains the top 15 languages used in GitHub repositories. 
- 

## How to run
### Local
1. Clone the repository.
2. Install the requirements.
3. Run the script you want to execute:
   1. Scripts can be run with the following command: `spark-submit scripts/{script_name}.py`. For example, to run the `top_15_languages` script, run `spark-submit scripts/top_15_languages.py`. 
   2. There's also a flag for testing purposes (-t or --test), which will run the script on a small subset of the data. For example, to run the `top_15_languages` script on a small subset of the data, run `spark-submit scripts/top_15_languages.py -t`.
   3. The logs of the script can be found in the `logs` folder. These provide a cleaner view of the script's execution.

## About the datasets
### Test dataset
The test dataset is a small subset of the original dataset. It is used for testing purposes and is not included in the final results.
The datasets were obtained using the following query on BigQuery and then downloaded as a .csv:

``SELECT * FROM `bigquery-public-data.github_repos. {table_name}` LIMIT 1000``

These can be found on the `resources` folder.

Some of these datasets had to be cleaned in order to be used, mostly because in BigQuery some values were stored as records (so when it downloaded, it downloaded as a column with a .json file). The cleaning scripts used can be found in the `src` folder. This was mostly useful for the scripts that used the table `languages`.