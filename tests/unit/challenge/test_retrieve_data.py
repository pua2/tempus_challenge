import datetime
import pytest
import os
import requests
import json
from dags import challenge as c


class TestSample:

    def test_make_dir(self):

        dir_path = os.getcwd()
        print(dir_path)
        received_dir = c.make_dir(dir_path,'csv')
        expected_dir = os.path.join(dir_path,'csv')
        assert expected_dir == received_dir
