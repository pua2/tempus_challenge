import datetime
import pytest
import os
import requests
import json
import pandas as pd

from dags import challenge as c


class TestSample:

    def test_make_dir(self):

        dir_path = os.getcwd()
        print(dir_path)
        received_dir = c.make_dir(dir_path,'csv')
        expected_dir = os.path.join(dir_path,'csv')
        assert expected_dir == received_dir

    def test_convert_to_df(self):
        test_data = {
            "status": "ok",
            "totalResults": 10,
            "articles": [
                {
                    "source": {
                        "id": "abc-news",
                        "name": "ABC News"
                    },
                    "author": "Joshua Hoyos, Meredith Deliso",
                    "title": "1 dead, 2 hurt in shooting outside Amazon fulfillment center in Jacksonville: Police",
                    "description": "",
                    "url": "https://abcnews.go.com/US/dead-hurt-shooting-amazon-fulfillment-center-jacksonville-police/story?id=71518255",
                    "urlToImage": "https://s.abcnews.com/images/US/amazon-jacksonville-pecan-park-road-google-street-view-ht-jc-200629_hpMain_16x9_992.jpg",
                    "publishedAt": "2020-06-29T21:21:51Z",
                    "content": "One person has been killed and another two injured in a shooting outside an Amazon fulfillment center in Jacksonville, Florida, police said.\r\nThis is a developing story. Please check back for updates."
                }
            ]
        }

        result_data = {'author':'Joshua Hoyos, Meredith Deliso',
                'content':'One person has been killed and another two injured in a shooting outside an Amazon fulfillment center in Jacksonville, Florida, police said.\r\nThis is a developing story. Please check back for updates.',
                'description':'',
                'publishedAt':'2020-06-29T21:21:51Z',
                'source_id':'abc-news',
                'source_name':'ABC News',
                'title':'1 dead, 2 hurt in shooting outside Amazon fulfillment center in Jacksonville: Police',
                'url':'https://abcnews.go.com/US/dead-hurt-shooting-amazon-fulfillment-center-jacksonville-police/story?id=71518255',
                'urlToImage':'https://s.abcnews.com/images/US/amazon-jacksonville-pecan-park-road-google-street-view-ht-jc-200629_hpMain_16x9_992.jpg'}

        result_df = pd.DataFrame(result_data, index=[0])

        test_df = c.Retrieval.convert_to_df(test_data)

        assert test_df.equals(result_df)
