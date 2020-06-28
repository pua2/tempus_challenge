"""
Tempus Challenge - Retrieve Data
Functions required to retrieve and transfer data to S3 Buckets
Handles challenge and bonus
"""

import requests
import os
import json
import pandas as pd
from pandas.io.json import json_normalize
import boto3
import os.path

#NEWS_API_KEY = '30eead0be8f841ce9041236db0d46a34'
#NEWS_API_KEY = 'a9a03a0d93f24948a3d41a5f4b69dde4'
#NEWS_API_KEY = '2842bef0853248eb849359cde0cc837e'
CURR_DIR = os.getcwd()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
#S3_BUCKET = os.getenv("S3_BUCKET")
#S3_BUCKET_BONUS = os.getenv("S3_BUCKET_BONUS")
#NEWS_API_KEY = 'a9a03a0d93f24948a3d41a5f4b69dde4'

def make_dir(path, name):
    """
    Creates directory if it does not exist

    :param str path: Path to new directory
    :param str name: name of directory
    :return: full directory path
    :rtype: str
    """

    dir = os.path.join(path,name)
    check_dir = os.path.isdir(dir)

    if not check_dir:
        os.makedirs(dir)
        print('New directory, {}, made in {}.'.format(name, path))
    else:
        print('{} directory already exists in {}.'.format(name, path))
    return dir

class Retrieval:

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
            url = 'https://newsapi.org/v2/sources?language={}&apiKey={}'.format(language, NEWS_API_KEY)

            print("Retrieving sources with language {} ...".format(language))
            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return "Error: " + str(e)

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
            url = 'https://newsapi.org/v2/top-headlines?sources={}&apiKey={}'.format(source_id, NEWS_API_KEY)

            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return "Error: " + str(e)

            r_json = response.json()

            # add top headlines to directory
            # key: source names
            # value: top headlines
            articles_pd = json_normalize(r_json['articles'])
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
            url = 'https://newsapi.org/v2/top-headlines?q={}&apiKey={}'.format(q, NEWS_API_KEY)

            print("Retrieving top headlines with keyword {} ...".format(q))
            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                return "Error: " + str(e)

            r_json = response.json()

            # add top headlines to directory
            # key: keywords
            # value: top headlines
            articles_pd = json_normalize(r_json['articles'])
            top_headlines[q] = articles_pd

        return top_headlines

    @classmethod
    def upload_to_S3(cls, **kwargs):
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
        print("Creating csv directory in {} ...".format(CURR_DIR))
        csv_dir = make_dir(CURR_DIR,'csv')

        # connect to S3 bucket
        print("Starting S3 connection ...")
        s3_client = boto3.client('s3')

        # loop through top headlines dict
        print("Creating csv files and uploading to {} bucket ...".format(bucket))
        for dir_name, top_headline in top_headlines.items():
            csv_filename = 'top_headlines.csv'
            csv_file_path = os.path.join(csv_dir,csv_filename)
            # create csv file
            # continuously replace new csv file with current file
            top_headline.to_csv(csv_file_path,index=False)

            s3_filename = '{}_top_headlines.csv'.format(execution_date)
            s3_path = '{}/{}'.format(dir_name, s3_filename)

            # upload file to S3 bucket
            s3_client.upload_file(csv_file_path, bucket, s3_path)

        print("Files uploaded to {} bucket.".format(bucket))
