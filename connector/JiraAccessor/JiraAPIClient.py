"""
JiraAPIClient.py
Jira API binding for Python 3.x.
Currently, this provides GET method support to get the data from Jira database.
POST method is not done for now.

Compatible with REST v2 and later.

Learn more:
https://developer.atlassian.com/cloud/jira/platform/rest/v2/intro/

"""

import base64

import requests
import configparser
import os
from pathlib import Path


curr_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
path = Path(curr_dir)
configFile = path.parent.absolute().__str__() + '\\etl_config.ini'
config.read(configFile)

class JiraAPIClient:
    def __init__(self):
        self.base_url = config.get('Jira', 'base_url')
        self.client_id = config.get('Jira',  'client_id')
        self.auth_token = config.get('Jira',  'AuthToken')

    def send_get(self, uri):
        """Issue a GET request (read) against the API.

        Args:
            uri: The API method to call including parameters, e.g. search?jql=project =RDL and type="Epic" &startAt=0.

        Returns:
            A dict containing the result of the request.
        """

        url = self.base_url + uri
        return self.__send_request('GET', url)

    def __send_request(self, method, url,data=''):

        auth = str(base64.b64encode(bytes(str(self.client_id) + ':' + str(self.auth_token), 'utf-8')), 'ascii').strip()
        headers = {
            'Authorization': 'Basic %s' %auth,
            'Content-Type':'application/json'
        }
        response = requests.get(url, headers=headers)

        if response.status_code > 201:
            try:
                error = response.json()
            except:     # response.content not formatted as JSON
                error = str(response.content)
            raise JiraAPIClientError('Jira API returned HTTP %s (%s)' % (response.status_code, error))
        else:
            try:
                return response.json()
            except: # Nothing to return
                return {}

class JiraAPIClientError(Exception):
    pass
