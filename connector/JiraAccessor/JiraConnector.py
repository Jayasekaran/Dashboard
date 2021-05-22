
"""This module handles the retrieve jira and
    Update the data into database

"""

import pyodbc
import requests
import json
import base64
import pandas as pd
from connector.JiraAccessor.JiraAPIClient import JiraAPIClient

class JiraConnector:
    """
    JiraConnector is an interface to get response data from jira using JiraClient
    It provide the methods to parse the response data to DataFrame objects.
    """
    def send_request(self, uri):
        """
        Make the get request for the given uri and return the response
        Args:
             uri
        Returns:
            response data for the given uri
        """
        startAt = 0
        total = 0
        issues = []
        try:
            jc = JiraAPIClient()
            while startAt <= total:
                uri_str = uri + ' &startAt=%s' %(str(startAt))
                res = jc.send_get(uri_str)
                startAt = startAt + res['maxResults']
                total =res['total']
                issues.extend(res['issues'])

        except Exception as e:
            error_msg = "[Error] Fail in send_request.\n :: %s" % e.__str__()
            print(error_msg)
        return issues

    def get_issues_by_labels(self, issueType : str, bug_labels: list, **kwargs):
        keys = list(kwargs.keys())
        values = list(kwargs.values())
        print('issueType=' + issueType)
        concate = ' AND ' + ''.join(key + '=' + value for key, value in zip(keys, values)) if kwargs else ''
        querystring = 'issueType=' + issueType + ' AND labels in (%s) %s' %(','.join(i for i in bug_labels), concate)
        response = self.send_request(querystring)
        return response

    def extract_epic(self, items):
        """
        Extract epic data and update the dataframe
        Args:
             list of Issues(Epic items)
        Returns:
            Epic Dataframe
        """
        data_set_epic = []
        try:
            for item in items:
                projName = item['fields']['project']['name'] if 'project' in item['fields'] else ''
                Key = item['key']
                issueType = item['fields']['issuetype']['name'] if 'issuetype' in item['fields'] else ''
                summary = item['fields']['summary'] if 'summary' in item['fields'] else ''
                status = item['fields']['status']['name'] if 'status' in item['fields'] else ''
                parentKey = item['fields']['customfield_11738'] if 'customfield_11738' in item['fields'] else ''
                epicName = item['fields']['customfield_10005'] if 'customfield_10005' in item['fields'] else ''
                print(Key)

                lbls = ''
                if 'labels' in item['fields'] and item['fields']['labels'] is not None:
                    labels = item['fields']['labels']
                lbls = ','.join(map(str, labels))
                data_set_epic.append([
                    projName,
                    Key,
                    issueType,
                    summary,
                    status,
                    parentKey,
                    epicName,
                    lbls
                ])
            dataset_epic = pd.DataFrame(data_set_epic)
            dataset_epic.columns = [
                'jv3_projects_base.NAME',
                'jv3_issues.KEY',
                'jv3_issues.ISSUE_TYPE',
                'jv3_issues.SUMMARY',
                'jv3_status.STATUS',
                'jv3_epic_parent.KEY',
                'jv3_epic_name',
                'labels'
            ]
        except Exception as e:
            error_msg = "[Error] Fail to extract epic data.\n :: %s" % e.__str__()
            print(error_msg)
        return dataset_epic

    def extract_story(self, items):
        """
         Extract stories and bugs data and update the dataframe
         Args:
             list of Issues(Story items)
         Returns:
             story and bugs Dataframe
         """
        data_set_story = []
        stories =[]
        bugs = []
        try:
            for item in items:
                projName = item['fields']['project']['name'] if 'project' in item['fields'] else ''
                storyKey = item['key']
                stories.append(storyKey)
                issueType = item['fields']['issuetype']['name'] if 'issuetype' in item['fields'] else ''
                summary = item['fields']['summary'] if 'summary' in item['fields'] else ''
                storyStatus = item['fields']['status']['name'] if 'status' in item['fields'] else ''
                parentKey = item['fields']['customfield_10006'] if 'customfield_10006' in item['fields'] else ''
                links = item['fields']['issuelinks'] if 'issuelinks' in item['fields'] else ''
                key = ''
                epic_name = item['fields']['customfield_10006'] if 'customfield_10006' in item['fields'] else ''
                story_id = item['id']
                updated = item['fields']['updated'] if 'updated' in item['fields'] else ''
                lbls = ''
                if 'labels' in item['fields'] and item['fields']['labels'] is not None:
                    labels = item['fields']['labels']
                lbls = ','.join(map(str, labels))

                data_set_story.append([
                    projName,
                    storyKey,
                    issueType,
                    summary,
                    storyStatus,
                    parentKey,
                    updated,
                    lbls
                ])
            dataset_story = pd.DataFrame(data_set_story)
            dataset_story.columns = [
                'jv3_projects_base.NAME',
                'jv3_issues.KEY',
                'jv3_issues.ISSUE_TYPE',
                'jv3_issues.SUMMARY',
                'jv3_status.STATUS',
                'jv3_story_parent.story_parent_key',
                'jv3_issue_updated.UPDATED',
                'jv3_issue.LABELS'
            ]

        except Exception as e:
            error_msg = "[Error] Fail to extract tests data.\n :: %s" % e.__str__()
            print(error_msg)
        return stories, dataset_story

    def extract_bugs(self, items):
        """
        Extract bug data and update the dataframe
        Args:
            bug -  bug data
        Returns:
            bug Dataset
        """
        data_set_issue = []
        try:
            for bug in items:
                if bug is not None:
                    comps = bug['fields']['components'] if 'components' in bug['fields'] else ''
                    issuecomps = ''
                    if len(comps) > 0:
                        for i in range(len(comps)):
                            issuecomps = issuecomps + ',' + comps[i]['name']
                    assignee = ''
                    if bug['fields']['assignee'] is not None:
                        assignee = bug['fields']['assignee']['name'] if 'assignee' in bug['fields'] else ''
                    lbls = ''
                    if 'labels' in bug['fields'] and bug['fields']['labels'] is not None:
                        labels = bug['fields']['labels']
                    lbls = ','.join(map(str, labels))
                    open('label_deb.txt', 'a').write(str(bug['key']) + str(lbls) + '\n')

                    resloution = ''
                    if 'resolution' in bug['fields'] and bug['fields']['resolution'] is not None:
                        resloution = bug['fields']['resolution']['name']
                    temp = ''
                    if 'customfield_10605' in bug['fields'] and bug['fields']['customfield_10605'] is not None:
                        temp = bug['fields']['customfield_10605']['value']

                    links = bug['fields']['issuelinks'] if 'issuelinks' in bug['fields'] else ''
                    if len(links) == 0:
                        links.append({'outwardIssue': {'key': '', 'fields':{ 'summary':''}}})
                    storyKey=''
                    storyName=''
                    for index in range(len(links)):
                        storyKey = links[index]['outwardIssue']['key'] if 'outwardIssue' in links[index] else None
                        storyName = links[index]['outwardIssue']['fields']['summary'] if 'outwardIssue' in links[index] else ''
                        if storyKey is None:
                            storyKey = links[index]['inwardIssue']['key'] if 'inwardIssue' in links[index] else ''
                            storyName = links[index]['inwardIssue']['fields']['summary']  if 'inwardIssue' in links[index] else ''

                    data_set_issue.append([
                        bug['fields']['project']['key'] if 'project' in bug['fields'] else '',
                        bug['fields']['project']['name'] if 'project' in bug['fields'] else '',
                        bug['fields']['issuetype']['name'] if 'issuetype' in bug['fields'] else '',
                        bug['key'],
                        bug['fields']['status']['name'] if 'status' in bug['fields'] else '',
                        resloution,
                        bug['fields']['status']['statusCategory']['name'] if 'statusCategory' in bug['fields'][
                            'status'] else '',
                        temp,
                        bug['fields']['summary'] if 'summary' in bug['fields'] else '',
                        bug['fields']['priority']['name'] if 'priority' in bug['fields'] else '',
                        bug['fields']['creator']['name'] if 'creator' in bug['fields'] else '',
                        assignee,
                        bug['fields']['created'] if 'created' in bug['fields'] else '',
                        issuecomps,
                        bug['fields']['updated'] if 'updated' in bug['fields'] else '',
                        lbls,
                        storyKey,
                        storyName
                    ])
            dataset_bugs = pd.DataFrame(data_set_issue)
            dataset_bugs.columns = [
                'jv3_projects_base.KEY',
                'jv3_projects_base.NAME',
                'jv3_issues.ISSUE_TYPE',
                'jv3_issues.KEY',
                'jv3_status.STATUS',
                'jv3_custom_single_select_fields.bug_resolution',
                'jv3_custom_single_select_fields.validation_status',
                'jv3_custom_single_select_fields.SEVERITY',
                'jv3_issues.SUMMARY',
                'jv3_issues.PRIORITY',
                'jv3_creator.CREATOR',
                'jv3_assignee.ASSIGNEE',
                'jv3_issues.CREATED',
                'jv3_components_list.components_list',
                'updated',
                'labels',
                'story_key',
                'story_name'
            ]
        except Exception as e:
            error_msg = "[Error] Fail to extract tests data.\n :: %s" % e.__str__()
            print(error_msg)
        return dataset_bugs

    def getBugs(self, url):
        """Issue a GET request (read) against the API to get
        the issues for given url,
        Args:
            url - url to the issue
        Returns:
            response as json issue data
        """

        try:
            cred = "Basic " + base64.b64encode(b'manigandan.s@hp.com:Nikath@06').decode("utf-8")
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": cred
            }
            headers = {
                'Authorization': 'Basic bWFuaWdhbmRhbi5zQGhwLmNvbTpvZERJVGRyWHUwaEswSU8yUWJkeUxpVFNoMHVPblkxT3Y0V1J2eA==',
                'Cookie': 'AWSALB=2juJ9z7fMwKa0f8V/bNJv7ffpdWySY9mylJp1x0njpT4UlESm3/XxeIf3Wtz63TBzySuumJR3MEgRA7XjW34ZtxJ5QvmENORtvL2MiwqcuxrcZAIfc0VXu1QPBRZ; AWSALBCORS=2juJ9z7fMwKa0f8V/bNJv7ffpdWySY9mylJp1x0njpT4UlESm3/XxeIf3Wtz63TBzySuumJR3MEgRA7XjW34ZtxJ5QvmENORtvL2MiwqcuxrcZAIfc0VXu1QPBRZ; JSESSIONID=0FE543BAC09BCD2DA8F0AD39A2A26B25; atlassian.xsrf.token=B6PH-08EC-IZ9H-PNPV_d5b06c157b23fc5483e239bbedbd23d042ae040b_lin'
            }

            # Send request and get response
            response = requests.request(
                "GET",
                url,
                headers=headers
            )
            if response.status_code == 200:
                return json.loads(response.text)

            else:
                return response.text
        except pyodbc.Error as ex:
            sqlstate = ex.args[1]
            print(sqlstate)

class JiraConnectionManagerError(Exception):
    pass
