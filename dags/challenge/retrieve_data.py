"""
Tempus Challenge - Retrieve Data
Functions required to retrieve and transfer data to S3 Buckets
Handles challenge and bonus
"""

import os
import os.path

import boto3
import requests
from pandas.io.json import json_normalize

CURR_DIR = os.getcwd()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")


def make_dir(path, name):
    """
    Creates directory if it does not exist

    :param str path: Path to new directory
    :param str name: name of directory
    :return: full directory path
    :rtype: str
    """

    dir = os.path.join(path, name)
    check_dir = os.path.isdir(dir)

    if not check_dir:
        os.makedirs(dir)
        print(f'New directory, {name}, made in {path}.')
    else:
        print(f'{name} directory already exists in {path}.')
    return dir


class Retrieval:

    def convert_to_df(top_headlines_json):
        """
        Converts json to df for articles_df

        :param dict top_headlines_json: json results of top headlines
        :return: articles as a DataFrame
        :rtype: DataFrame
        """
        articles_df = pd.DataFrame()

        # split sources into seperate fields
        for articles in top_headlines_json['articles']:
            articles['source_name'] = articles['source']['name']
            articles['source_id'] = articles['source']['id']

            del articles['source']

            articles_df = articles_df.append(articles, ignore_index=True)

        return articles_df

    @classmethod
    def get_sources_lang(cls, *args):
        """
        Retrieve source ids and source names from newsapi.org

        :param list args: List of languages
        :return: source id and source names
        :rtype: dict

        """
        source_info = {}

        print("Retrieving sources ...")
        # loop through list of languages
        for language in args:
            url = f'https://newsapi.org/v2/sources?language={language}&apiKey={NEWS_API_KEY}'

            print(f"Retrieving sources with language {language} ...")
            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return f"Error: {e}"

            r_json = response.json()
            news_sources = r_json['sources']

            # add source id and name from json to dict
            for source in news_sources:
                if source['id'] not in source_info:
                    source_info[source['id']] = source['name']

        return source_info

    @classmethod
    def get_headlines(cls, **context):
        """
        Retrieve top headlines data from source ids

        :return: top headlines by source name
        :rtype: dict
        """

        # get returned dict from retrieve_sources_from_lang
        ti = context['ti']
        source_info = ti.xcom_pull(task_ids='retrieve_sources_from_lang')

        top_headlines = {}

        print("Retrieving top headlines ...")
        # loop through source dict
        for source_id, source_name in source_info.items():
            url = f'https://newsapi.org/v2/top-headlines?sources={source_id}&apiKey={NEWS_API_KEY}'

            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return f"Error: {e}"

            r_json = response.json()
            articles_pd = convert_to_df(r_json)

            # add top headlines to directory
            # key: source names
            # value: top headlines
            top_headlines[source_name] = articles_pd

        return top_headlines

    @classmethod
    def get_headlines_q(cls, *args):
        """
        Retrieve top headlines from keywords

        :param list args: list of all keywords
        :return: top headlines by keyword
        :rtype: dict
        """
        top_headlines = {}

        print("Retrieving top headlines ...")
        # loop through keywords
        for q in args:
            url = f'https://newsapi.org/v2/top-headlines?q={q}&apiKey={NEWS_API_KEY}'

            print(f"Retrieving top headlines with keyword {q} ...")
            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return f"Error: {e}"

            r_json = response.json()
            articles_pd = convert_to_df(r_json)

            # add top headlines to directory
            # key: keywords
            # value: top headlines
            top_headlines[q] = articles_pd

        return top_headlines

    @classmethod
    def upload_to_s3(cls, **kwargs):
        """
        Create csv from top headlines and upload to S3 Buckets
        """

        # use top headlines dict from previous task
        ti = kwargs['ti']
        top_headlines = ti.xcom_pull(task_ids='retrieve_headlines')

        # get exection date from airflow context
        execution_date = kwargs['ds_nodash']
        bucket = kwargs['S3_BUCKET']

        # create directory to store csv
        print(f"Creating csv directory in {CURR_DIR} ...")
        csv_dir = make_dir(CURR_DIR, 'csv')

        # connect to S3 bucket
        print("Starting S3 connection ...")
        s3_client = boto3.client('s3')

        # loop through top headlines dict
        print(f"Creating csv files and uploading to {bucket} bucket ...")
        for dir_name, top_headline in top_headlines.items():
            csv_filename = 'top_headlines.csv'
            csv_file_path = os.path.join(csv_dir, csv_filename)
            # create csv file
            # continuously replace new csv file with current file
            top_headline.to_csv(csv_file_path, index=False)

            s3_filename = f'{execution_date}_top_headlines.csv'
            s3_path = f'{dir_name}/{s3_filename}'

            # upload file to S3 bucket
            s3_client.upload_file(csv_file_path, bucket, s3_path)

        print(f"Files uploaded to {bucket} bucket.")
