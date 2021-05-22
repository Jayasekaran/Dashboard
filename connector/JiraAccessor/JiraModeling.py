
import pandas as pd
from connector.JiraAccessor.JiraConnector import JiraConnector
from connector.etl import Etl

class JiraModeling (Etl):
    """This module handles the retrieve jira and
        Update the data into database
    """
    def get_issues_by_labels(self, issueType : str, bug_labels: list, **kwargs):
        jctor = JiraConnector()
        keys = list(kwargs.keys())
        values = list(kwargs.values())
        print('issueType=' + issueType)
        concate = ' AND ' + ''.join(key + '=' + value for key, value in zip(keys, values)) if kwargs else ''
        querystring = 'issueType=' + issueType + ' AND labels in (%s) %s' %(','.join(i for i in bug_labels), concate)
        response = jctor.send_request(querystring)
        return response

    def extract(self):
        """ to get all the issues for given project based on the filter argument
        with '' (empty) will get all the issues, otherwise get all
        the data updated during the given period ('1h'- last 1 hour update record)
        Returns:
            none
        """

        ###
        # period that we need to get the data from jira

        config = self.getConfig()
        update_filter=''
        filter = config.get('Jira', 'updated_time')
        if filter != '':
            update_filter = 'AND updated >= -{}'.format(filter)

        projectKey = config.get('Jira', 'project_key')
        try:
            jctor = JiraConnector()
            #get EPIC data
            uri = 'search?jql=project ={0} and type="Epic"{1}'.format(projectKey, update_filter)
            data= jctor.send_request(uri)
            df_epic = jctor.extract_epic(data)

            #Stories data
            uri = 'search?jql=project = {0} and type in (Story) and "Epic Link" in (RDL-675, RDL-2577) {1}'.format(
                projectKey, update_filter)
            data= jctor.send_request(uri)
            stories, df_story = jctor.extract_story(data)
            pf_bugs = pd.DataFrame()

            #Bugs data
            li=[]
            for story in stories:
                li.append("linkedIssues({0})".format(story))

            lis = ','.join(map(str, li))
            uri = 'search?jql= issue in ({0}) and type in (Bug) {1}'.format(lis, update_filter)
            data= jctor.send_request(uri)
            df_bugs= jctor.extract_bugs(data)

            return df_epic, df_story, df_bugs

        except Exception as e:
            error_msg = "[Error] Fail to extract jira data.\n :: %s" % e.__str__()
            print(error_msg)
        return False,False,False

class JiraModelingError(Exception):
    pass
