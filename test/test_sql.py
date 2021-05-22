#-*-coding:utf-8-*-

# python default unittest. Please refer below guide
# https://docs.python.org/3/library/unittest.html
# https://docs.python.org/3/library/unittest.html#assert-methods

import unittest
import os

from connector.SQLManager import *

import warnings

class Testclass(unittest.TestCase):
    """
    Test testcase running
    """
    lookid_initiative= 406

    def test_init(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        db_ins = None
        db_ins = SQLManager()       
          
        self.assertIsNotNone(db_ins)
        
    def test_basic(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        db_ins = None
        db_ins = SQLManager()
        
        sample_data = [(1, 'test'), (2, '한글'), (3, 'english 한글'), (4, 'update')]
        
        query = 'select * from sample1'
        rt = db_ins.runQueryAutoCommit(query)
        self.assertTrue( str(sample_data).strip('[]') == str(rt).strip('[]') )
        
        table_qeury = """CREATE TABLE %s(                
                [jira_issues.KEY] varchar(100),
                [jira_issues.SUMMARY]  nvarchar(1000),
            );""" % 'sample_table'
            
        print('test table exists')
        rt = db_ins.createTable('sample1', table_qeury)
        self.assertFalse(rt)
            
        print('test create table')
        rt = db_ins.createTable('sample_table', table_qeury)
        self.assertTrue(rt)
        
        print('test confirm to create table')
        confrim_sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = 'sample_table'"
        rt = db_ins.runQueryAutoCommit(confrim_sql)
        self.assertIsNotNone(rt)
        print(rt)
        
        print('test drop table')
        rt = db_ins.runQuery('DROP TABLE sample_table')
        self.assertTrue(rt)
        
        print('test commit')
        rt = db_ins.commit()
        self.assertTrue(rt)
        
        print('test confirm to drop table')
        rt = db_ins.runQueryAutoCommit(confrim_sql)        
        self.assertIsNone(rt)
        print(rt)
        
        
    