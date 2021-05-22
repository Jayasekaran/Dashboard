#-*-coding:utf-8-*-
import unittest
import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from connector.TestrailConn import *

def main():
    rt = testrail_connector()
    rs = rt.get_projects()
    rs = rt.get_milestones(180)
    rs = rt.get_milestone(3962)
    rs = rt.get_plans(180, 3963)
    rs = rt.get_plan(109108)
    rs = rt.get_tests(111582)
    print("\nauthentication test result :: " + str(rs))

main()