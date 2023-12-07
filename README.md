# Github-Analytica
## Introduction
GitHub, the go-to hub for developers worldwide, has seen a whopping 12 million contributors shaping around 31 million projects since 2008. It's the busy epicenter of open-source development.

Now, picture this: you're a student navigating the coding universe or someone just stepping into a new stack. GitHub, with its countless projects, feels like a vast library without a roadmap. The challenge here is real - finding your way through the code maze.

So, what's the plan? We're diving into the GitHub realm to decode the secrets. What programming languages are dominating the scene and what trusty sidekicks (dependencies) are being used.

Why bother, you ask? For students grasping coding nuances or those embarking on a new tech stack, it's like having a guidebook. We aim to demystify GitHub, making it more navigable, and bring you insights into the coding practices that define different languages.  Ready to jump into the GitHub adventure with us? 🚀

## Run requirements
- Python 3.9
- Pip
- PySpark 

## Architecture


## Project structure

The project consists of x number of scripts, each with a specific purpose. They are built from an interface in order to streamline the coding process.

The scripts, and a brief explanation of what they do, are as follows:

- 

## About the datasets
### Test dataset
The test dataset is a small subset of the original dataset. It is used for testing purposes and is not included in the final results.
The datasets were obtained using the following query on BigQuery and then downloaded as a .csv:

``SELECT * FROM `bigquery-public-data.github_repos. {table_name}` LIMIT 1000``

These can be found on the `resources` folder.
