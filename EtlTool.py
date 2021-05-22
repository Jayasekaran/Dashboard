'''
    Jira Incremental update is used to retrieve data from with specified interval of time.
    this application will connect Jira get the required data in given period and
    update them in to SQL server.

    Author: Jayasekaran
    Date : 2/09/2021
'''

from connector.JiraAccessor.JiraModeling import *
from connector.JiraAccessor.JiraConnector import *
from connector.TestrailAccessor.TestrailModeling import *


def get_testrail_data():
    tm = TestrailModeling()
    isExtract = tm.extract()
    if isExtract:
        tm.submitSQL(tm.df_projects, 'TestrailProjects')
        tm.submitSQL(tm.df_milestones, 'TestrailMilestones')
        tm.submitSQL(tm.df_submilestones, 'TestrailSubmilestones')
        tm.submitSQL(tm.df_plans, 'TestrailPlans')
        tm.submitSQL(tm.df_runs, 'TestrailRuns')
        if not tm.df_results.empty:
            tm.submitSQL(tm.df_results, 'TestrailResults')
        if not tm.df_tests.empty:
            tm.submitSQL(tm.df_tests, 'TestrailTests')
        if not tm.df_sections.empty:
            tm.submitSQL(tm.df_sections, 'TestrailSections')
        if not tm.df_cases.empty:
            tm.submitSQL(tm.df_cases, 'TestrailCases')
        if not tm.df_summary.empty:
            tm.submitSQL(tm.df_summary, 'TestrailSummary')

def get_jira_data():
    jira = JiraModeling()
    # get epic from jira --Later
    df_epic, df_story, df_bugs = jira.extract()

    jira.submitSQL(df_epic,"JiraEpic")
    jira.submitSQL(df_story,"JiraStory")
    jira.submitSQL(df_epic,"JiraIssues")

def get_jira_issues_CSIT():
    jira = JiraConnector()
    jira_model= JiraModeling()
    issueType = 'Bug'
    labels = ['WF','JRNY','IPT']
    common_label = 'SIT'
    data = jira.get_issues_by_labels(issueType, labels, labels=common_label)
    latest_bugs = jira.extract_bugs(data)
    print(latest_bugs)
    jira_model.submitSQL(latest_bugs, 'APIIssues_SIT', db='testDB')

if __name__ == '__main__':
    #Jira
    # get_jira_data()
    #Testrail
    get_testrail_data()
    # get_jira_issues_CSIT()
