#-*-coding:utf-8-*-
import unittest
import os, sys
import pandas as pd
from connector.TestrailAccessor.TestrailConnector import *
from connector.SQLManager import *
from connector.etl import Etl

rt = TestrailConnector()

class TestrailModeling (Etl):
    def __init__(self):
        self.df_projects = pd.DataFrame()
        self.df_milestones = pd.DataFrame()
        self.df_submilestones = pd.DataFrame()
        self.df_plans = pd.DataFrame()
        self.df_plan = pd.DataFrame()
        self.df_runs = pd.DataFrame()
        self.df_cases = pd.DataFrame()
        self.df_tests = pd.DataFrame()
        self.df_sections = pd.DataFrame()
        self.df_results = pd.DataFrame()
        self.df_summary = pd.DataFrame()

    def extract(self):
        ret_value= False
        submilestone_list = []
        plans_list=[]
        runs_list=[]
        cases_list=[]
        sections_list=[]
        results_list=[]
        tests_list = []
        project_list, projName = self.getTestrailProjectKey()
        try:
            #Get Projects  Data into the self.df_projects as a dataframe  ##Start
            rs = rt.get_data('get_projects')
            if rs['errorCode'] != 200:
                print(rs['errorMsg'])
            rs = rs["contents"]
            col, data = self.parse_data(rs)
            self.df_projects = pd.DataFrame(data)
            self.df_projects.columns=col
            ##Projects End

            #Get Milestones  Data into the self.df_milestones as a dataframe  ##Start
            milestone_list=[]
            for project_id in project_list:
                rs = rt.get_data('get_milestones/%d' % (project_id))
                rs = rs["contents"]
                col,data = self.parse_data(rs)
                milestone_list=milestone_list +data
                self.df_milestones = pd.DataFrame(milestone_list)
                self.df_milestones.columns = col
                ##Milestones End

                # Get SubMilestones  Data into the self.df_submilestones as a dataframe  ##Start
                for milestone_id in self.df_milestones['id']:
                    if milestone_id == 5502:
                        print(milestone_id)
                        rs = rt.get_data('get_milestone/%d' % (milestone_id))
                        rs = rs["contents"]["milestones"]
                        col, data = self.parse_data(rs)
                        submilestone_list = submilestone_list+data
                if milestone_id == 5502:
                    self.df_submilestones = pd.DataFrame(submilestone_list)
                    self.df_submilestones.columns = col
                ##SubMilestone End

                # Get Plans  Data into the self.df_plans as a dataframe  ##Start
                col=[]
                for submilestone_id, project_id in zip(self.df_submilestones['id'],
                                                       self.df_submilestones['project_id']):
                    rs = rt.get_data('get_plans/%d&milestone_id=%d' % (project_id, submilestone_id))
                    rs = rs["contents"]
                    colm, data = self.parse_data(rs)
                    if len(colm) >0:
                        col = colm
                    plans_list = plans_list + data
                self.df_plans = pd.DataFrame(plans_list)
                self.df_plans.columns = col
                ##Plans End

                # Get Runs  Data into the self.df_runs as a dataframe  ##Start
                col=[]
                for plan_id in self.df_plans['id']:
                    rs = rt.get_data('get_plan/%d' % (plan_id))
                    rs = rs["contents"]["entries"]
                    for index in range(len(rs)):
                        for run_index in range(len(rs[index]['runs'])):
                            colm, data = self.parse_data(rs[index]['runs'])
                            if len(colm) >0:
                                col = colm
                            runs_list = runs_list + data
                self.df_runs = pd.DataFrame(runs_list)
                self.df_runs.columns = col

                col_summary = ['run_id', 'milestone_id','project_id','ucde_or_nonucde', 'feature', 'test_area', 'env1', 'env2', 'env3', 'passed_count',
                               'blocked_count', 'untested_count', 'retest_count', 'failed_count', 'custom_status1_count',
                               'custom_status2_count', 'custom_status3_count','custom_status4_count','custom_status5_count',
                               'custom_status6_count', 'custom_status7_count','custom_status6_count', 'custom_status7_count']
                summary=[]
                for index in self.df_runs.index:
                    run_id = self.df_runs['id'][index]
                    run_name = self.df_runs['name'][index]
                    suite_id = self.df_runs['suite_id'][index]
                    names = run_name.split('_')
                    hasNewStruct=False
                    hasOldStruct=False
                    if len(names) == 3:
                        hasNewStruct=True
                        if  self.df_runs['config'][index] != None:
                            configs = self.df_runs['config'][index].split()
                            env1=''
                            env2=''
                            env3=''
                            if len(configs) == 3:
                                env1 = configs[0]
                                env2 = configs[1]
                                env3 = configs[2]

                            summary.append([
                                self.df_runs['id'][index],
                                self.df_runs['milestone_id'][index],
                                self.df_runs['project_id'][index],
                                names[0],
                                names[1],
                                names[2],
                                env1,
                                env2,
                                env3,
                                self.df_runs['passed_count'][index],
                                self.df_runs['blocked_count'][index],
                                self.df_runs['untested_count'][index],
                                self.df_runs['retest_count'][index],
                                self.df_runs['failed_count'][index],
                                self.df_runs['custom_status1_count'][index],
                                self.df_runs['custom_status2_count'][index],
                                self.df_runs['custom_status3_count'][index],
                                self.df_runs['custom_status4_count'][index],
                                self.df_runs['custom_status5_count'][index],
                                self.df_runs['custom_status6_count'][index],
                                self.df_runs['custom_status7_count'][index],
                                self.df_runs['custom_status6_count'][index],
                                self.df_runs['custom_status7_count'][index]]
                               )
                    else:
                        hasOldStruct=True

                        rs = rt.get_data('get_tests/%d' % (run_id))
                        rs = rs["contents"]
                        col, data = self.parse_data(rs)
                        tests_list = tests_list + data

                        rs = rt.get_data('get_results_for_run/%d' % (run_id))
                        rs = rs["contents"]
                        col, data = self.parse_data(rs)
                        results_list = results_list + data
                    #Cases Start
                        rs = rt.get_data('get_cases/%d&suite_id=%d' % (project_id, suite_id))
                        rs = rs["contents"]
                        col, data = self.parse_data(rs)
                        cases_list = cases_list + data

                        rs = rt.get_data('get_sections/%d&suite_id=%d' % (project_id, suite_id))
                        rs = rs["contents"]
                        col, data = self.parse_data(rs)
                        sections_list = sections_list + data
            if hasOldStruct:
                self.df_tests = pd.DataFrame(tests_list)
                self.df_tests.columns = col

                self.df_results = pd.DataFrame(results_list)
                self.df_results.columns = col

                self.df_runs = pd.DataFrame(runs_list)
                self.df_runs.columns = col

                self.df_cases = pd.DataFrame(cases_list)
                self.df_cases.columns = col

                self.df_sections = pd.DataFrame(sections_list)
                self.df_sections.columns = col
            if hasNewStruct:
                self.df_summary = pd.DataFrame(summary)
                self.df_summary.columns = col_summary
            ret_value = True
        except Exception as e:
            error_msg = "[Error] Fail to extract milestones data.\n :: %s" % e.__str__()
            print(error_msg)
        return ret_value

    def parse_data(self, rs ):
        try:
            data_set=[]
            col=[]
            for index in range(len(rs)):
                val = []
                for key, value in rs[index].items():
                    if index == 0 or len(col) == 0:
                        col.append(key)
                    val.append(value)
                data_set.append(val)
        except Exception as e:
            error_msg = "[Error] Fail to parse data.\n :: %s" % e.__str__()
            print(error_msg)
        return col, data_set

