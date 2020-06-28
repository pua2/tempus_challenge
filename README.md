<<<<<<< HEAD
# Tempus Data Engineer Challenge
For this challenge,
 you will develop a simple
 [Apache Airflow](https://airflow.apache.org) data pipeline.

## Challenge Summary
Our data pipeline must fetch data from [News API](https://newsapi.org),
 transform the data into a tabular structure,
 and store the transformed data on [Amazon S3](https://aws.amazon.com/s3/).

## Setup
1. Add AWS credentials to ./.aws/credentials.
2. Add News API key and S3 bucket names to ./.env file.
    * docker-compose adds these variables to environment

## Quickstart
1. Create virtualenv using Python 3.6.8.
    * docker versions are docker 19.03.8 and docker-compose 1.25.5.
2. Run `make init` to download project dependencies.
3. Run `make test` to make sure basic smoke tests are passing.
4. Run `make run` with docker running to bring up airflow.
    * The Airflow UI/Admin Console should now be visible on [http://localhost:8080](http://localhost:8080).

## Pipelines
### tempus_challenge_dag
1. retrieve_sources_from_lang: PythonOperator used to retrieve sources from newsapi.org.
    * This task passes in a list of languages.
    * A dictionary of sources (source_id:source_name) is returned.
2. retrieve_headlines: PythonOperator used to retrieve headlines from newsapi.org.
    * Source's are passed from previous task and inputted to newsapi.org.
    * A dictionary of top headlines (source_name:top_headline) is returned.
3. upload_to_S3: PythonOperator used to create csv and upload to S3 Buckets.
    * Top headlines are formatted into csv files
    * Each file is uploaded into a separate directory. Each directory is named after the source name.
4. end_task: DummyOperator used to show end of task.

### tempus_challenge_bonus_dag
This pipeline is set to run daily
1. retrieve_headlines: PythonOperator used to retrieve headlines from newsapi.org.
    * This task passes a list of keywords.
    * A dictionary of top headlines (keyword:headline) is returned.
2. upload_to_S3: PythonOperator used to create csv and upload to S3 Buckets.
    * Top headlines are formatted into csv files
    * Each file is uploaded into a separate directory. Each directory is named after the keyword.
3. end_task: DummyOperator used to show end of task.


## Requirements
- [ ] Use Airflow to construct a new data pipeline (DAG) named 'tempus_challenge_dag'.
- [ ] Data pipeline must be scheduled to run once a day.
- [ ] Data pipeline will:
  - [ ] Retrieve all English news sources.
  - [ ] For each news source, retrieve the top headlines.
    - [ ] Top headlines must be flattened into a CSV file. CSV Filename: `<pipeline_execution_date>_top_headlines.csv`
    - [ ] Result CSV must be uploaded to the following s3 location `<s3_bucket>/<source_name>`
- [ ] The solution must contain at least one test for your headline transformation.
- [ ] The solution must be start-able via `make run`.
- [ ] The solution must be pep-8 compliant.
- [ ] Bonus: Build a separate pipeline that uses the following keywords instead of English news sources: Tempus Labs, Eric Lefkofsky, Cancer, Immunotherapy
- [ ] Bonus: Write an integration test for any of the external services your solution connects to.


## Rules of engagement
* We suggest that you establish a four hour timebox to complete the challenge.
* The solution must perform a Python transformation of the data;
 feel free to add any open-source libraries you wish and add additional output files.
* Please document changes required to make the solution resilient to
 failure by taking the following actions:
  * add developer-friendly requirements to functions
  * add comments in the main function that list failures that the solution should
  be designed to handle
* Please run `make clean` and deliver your Python code via repo or zip ahead of the meeting.

## Grading
We will grade your solution with the following guidelines.
 This list is ordered with highest-weighted factors at the top:
1. **Functional correctness**: The solution meets all functional requirements,
 including bonuses.
2. **Code composition and style**: Code follows appropriate coding standards and pep-8 guidelines.
3. **Communication**: The project includes a README and the code is well-commented.


## [Apache Airflow](https://airflow.apache.org)
From the Apache Airflow documentation:

>Airflow is a platform to programmatically author, schedule and monitor workflows.

>Use airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

>When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

In order to facilitate the use of Airflow, we have included a Dockerfile and a docker-compose.yml that can be used to set up a local airflow development environment. **Make sure to have [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.**

From the root folder, you can execute the following command to run airflow:
```
docker-compose up --build
```
The Airflow UI/Admin Console should now be visible on [http://localhost:8080](http://localhost:8080).

### DAGs
In order to build the data pipeline, it will be necessary to create a DAG. We have provided an example DAG, `dags/sample_dag.py`, that can be used as a reference. Further documentation can be found in the [airflow tutorial](https://airflow.apache.org/tutorial.html) and the [airflow concepts](https://airflow.apache.org/concepts.html) pages.

To load a new DAG into airflow, simply create a new Python file in the `dags` folder that contains an airflow DAG object.

### Python Packages
To install additional Python packages (boto3, pandas, requests, etc.), add them to `requirements.txt`.

### Related Articles/Tutorials
* https://airflow.apache.org/index.html
* https://medium.com/@dustinstansbury/understanding-apache-airflows-key-concepts-a96efed52b1a
* https://speakerdeck.com/artwr/apache-airflow-dataengconf-sf-2017-workshop
* https://github.com/hgrif/airflow-tutorial

## [News API](https://newsapi.org)
A simple REST API that can be used to retrieve breaking headlines and search for articles. **A free News API account is required to obtain an API key.**

| Route             | Description                                                                                                                |
|-------------------|----------------------------------------------------------------------------------------------------------------------------|
| [/v2/top-headlines](https://newsapi.org/docs/endpoints/top-headlines) | Returns live top and breaking headlines for a country, specific category in a country, single source, or multiple sources. |
| [/v2/sources](https://newsapi.org/docs/endpoints/sources) | Returns the subset of news publishers that top headlines are available from. |


## [Amazon S3](https://aws.amazon.com/s3/)
A simple cloud storage service run by Amazon Web Services (AWS). **An AWS account is needed to use AWS S3. Furthermore, AWS has a [free tier](https://aws.amazon.com/free/) that can be used for this challenge.**

Amazon provides a Python SDK (**[boto](http://boto3.readthedocs.io/en/latest/guide/resources.html)**), that provides an easy to use API for interacting with AWS S3.
