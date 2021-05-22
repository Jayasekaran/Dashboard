
"""This module handles the retrieve jira and
    Update the data into database

"""
import pandas as pd
from connector.SQLManager import SQLManager
from pathlib import Path
import configparser
import os

class Etl:

    def getConfig(self):
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        config = configparser.ConfigParser()
        path = Path(curr_dir)
        configFile = curr_dir + '\\etl_config.ini'
        try:
            config.read(configFile)
        except (OSError, IOError) as e:
            error_msg = "[Error] Fail to read etl_config.ini.\n :: %s" % e.__str__()
            print(error_msg)
        return config

    def getTestrailProjectKey(self):
        K = [119, 164, 182, 225]
        T = [180]
        config = self.getConfig()
        projectKey = config.get('Testrail', 'project_key').replace('-', '').replace('_', '').replace(' ', '')
        if projectKey.lower() == 'sivk':
            return K, projectKey
        elif projectKey.lower() == 'sivt':
            return T, projectKey
        else:
            return None


    def getTableFullName (self, name):
        # config = self.getConfig()
        # projectKey = config.get('Testrail', 'project_key')
        projId, projName = self.getTestrailProjectKey()
        return projName +'_'+ name

    def submitSQL(self, df, tableName, db=''):
        """update the table with given dataframe.
        Args: df to update, table name
        Returns:
        """

        tableName= self.getTableFullName(tableName)
        db_ins = SQLManager() if not db else SQLManager(db)
        exist_statement = 'SELECT COUNT(*) FROM %s' % tableName
        rows = db_ins.runQuery(exist_statement)
        if rows == 0:
            create_statement = 'CREATE TABLE %s (dummy int NOT NULL)' % tableName
            try:
                db_ins.runQuery(create_statement)
            except Exception as e:
                error_msg = "[Error] Fail to create table.\n :: %s" % e.__str__()
                print(error_msg)

        duplicate_statement = 'SELECT * INTO temp_tr FROM %s' % tableName
        db_ins.runQuery(duplicate_statement)
        delete_statement = 'DROP TABLE temp_tr'

        try:
            df.to_sql(name=tableName, con=db_ins.engine, if_exists='replace', index=False)
            print('Data loaded to %s' %tableName)
            db_ins.runQuery(delete_statement)
        except Exception as e:
            restore_statement = 'CREATE TABLE %s SELECT * FROM temp_tr' % tableName
            db_ins.runQuery(restore_statement)
            db_ins.runQuery(delete_statement)
            error_msg = "[Error] Fail to submit data to table.\n :: %s" % e.__str__()
            print(error_msg)

    def selectSQL(self, tableName):
        """get all the data from the table query.
        Args: tablename
        Returns: table data as dataframe
        """

        tableName = self.getTableFullName(tableName)
        db_ins = SQLManager()
        df = pd.read_sql_table(table_name=tableName, con=db_ins.engine)
        return df

        exist_statement = 'SELECT COUNT(*) FROM %s' % tableName
        rows = db_ins.runQuery(exist_statement)
        if rows == 0:
            create_statement = 'CREATE TABLE %s (dummy int NOT NULL)' % tableName
            try:
                db_ins.runQuery(create_statement)
            except Exception as e:
                error_msg = "[Error] Fail to create table.\n :: %s" % e.__str__()
                print(error_msg)

        duplicate_statement = 'SELECT * INTO temp_tr FROM %s' % tableName
        db_ins.runQuery(duplicate_statement)
        delete_statement = 'DROP TABLE temp_tr'

        try:
            df.to_sql(name=tableName, con=db_ins.engine, if_exists='replace', index=False)
            db_ins.runQuery(delete_statement)
        except Exception as e:
            restore_statement = 'CREATE TABLE %s SELECT * FROM temp_tr' % tableName
            db_ins.runQuery(restore_statement)
            db_ins.runQuery(delete_statement)
            error_msg = "[Error] Fail to submit data to table.\n :: %s" % e.__str__()
            print(error_msg)

    def checkTableExists(self, tableName):
        tableName = self.getTableFullName(tableName)
        db_ins = SQLManager()
        return db_ins.checkTableExists(tableName)

class EtlError(Exception):
    pass
