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
3. upload_to_S3: PythonOperator used to create csv and upload to S3 buckets.
    * Top headlines are formatted into csv files
    * Each file is uploaded into a separate directory. Each directory is named after the source name.
    * File names are labeled as 'yyyyMMdd_top_headlines.csv'
    * Empty files will also be uploaded to ensure no days are missed.
4. end_task: DummyOperator used to show end of task.

### tempus_challenge_bonus_dag
This pipeline is set to run daily
1. retrieve_headlines: PythonOperator used to retrieve headlines from newsapi.org.
    * This task passes a list of keywords.
    * A dictionary of top headlines (keyword:headline) is returned.
2. upload_to_S3: PythonOperator used to create csv and upload to S3 buckets.
    * Top headlines are formatted into csv files
    * Each file is uploaded into a separate directory. Each directory is named after the keyword.
    * File names are labeled as 'yyyyMMdd_top_headlines.csv'
    * Empty files will also be uploaded to ensure no days are missed.
3. end_task: DummyOperator used to show end of task.

## Additional Notes
- [ ] Environment variables were added to docker-compose file to handle api key and bucket names
- [ ] Additional line '&& pip install werkzeug==0.16.1 \' was added to Dockerfile. This was necessary to handle issue with running older version of airflow.
