import os
import configparser
import logging

from connector.TestrailAccessor.TestrailAPIClient import TestrailAPIClient
from connector.etl import Etl

logger = logging.getLogger('SIVDashboard')
curr_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(curr_dir + '\\etl_config.ini')
etl=Etl()
config = etl.getConfig()

class TestrailConnector:
    """
    Preperation object for getting data using APIs
    """
    client = TestrailAPIClient(config.get('Testrail','base_url'))
    def __init__(self):
        """
        From ini file, Get the Credential Information for Testrail
        """
        self.client.user = config.get('Testrail','client_id')
        self.client.password = config.get('Testrail','AuthToken')

    def get_data(self, url):
        """
        Get all information based on the url
        Argument : url
        """
        case={}
        try:
            contents = self.client.send_get(url)
            case['contents'] = contents
            case['errorMsg']= 'Successfully retrieved data for {}'.format(url)
            case['errorCode'] = 200
        except Exception as e:
            logger.debug(e)
            e = str(e)
            case['contents'] = {}
            case['errorMsg'] = e
            case['errorCode'] = -1
            print(case)
        finally:
            return case
