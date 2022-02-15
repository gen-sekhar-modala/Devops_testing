#from google.cloud import storage
print("yes")
from google.cloud import bigquery
import pandas as pd
import base64
import ast, os
import logging as logger
import time
import traceback
import json
import datetime
#from flask import request

CREATE_TABLE = ("SELECT * FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.TABLES` WHERE table_name= '{TABLE_NAME}' ;")
CREATE_SP = ("SELECT * FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.ROUTINES` WHERE routine_name= '{TABLE_NAME}';")
#CREATE_VIEW = ("SELECT table_catalog,table_schema,	table_name,	view_definition  FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.VIEWS` WHERE table_name= '{TABLE_NAME}';")
CREATE_VIEW = ("SELECT *  FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.TABLES` WHERE table_name= '{TABLE_NAME}';")
CREATE_TABLE_TAR = ("SELECT * FROM  `{TARGET_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.TABLES` WHERE table_name= '{TABLE_NAME}' ;")
BACKUP_TABLE=("CREATE OR REPLACE TABLE `{TARGET_PROJECT_NAME}.{BACKUP_DATASET}.{TABLE_NAME}` as SELECT * FROM `{SOURCE_PROJECT_NAME}.{DATASET).{TABLE_NAME}`;")
def execute_query(QUERY):
    try:
        client = bigquery.Client()
        query_job = client.query(QUERY)    
        print("Execute_query......")
        rows = query_job.result()
    except:
        print("ENTITY NOT FOUND For query job")
        raise
    
    return rows


def deploy_ddl(sql):
    try:
        print("sql............",sql)
        result_list = []
        SQL_QUERY = ''
        print("deploy_ddl....")
        client = bigquery.Client()
        job = client.query(sql)
        job.result()
    except:
        print("Error while deploying DDL ....")
        raise

def create(filename,txt):
    f = open("C:\\Devops\\Devops_testing\\sql\\"+filename+".sql","w")
    f.write(txt)
    f.close()
    
#def hello_world(request):
def hello_world():#data-engineering-gcp-practice:Example_dataset1_stg.Emp_details
    print("yes")
    try:  
        data = '{"ENTITY_TYPE":"TABLE","DATASET_NAME":"Example_dataset1_stg","TARGET_DATASET_NAME":"Example_dataset1","ENTITY_NAME":"Emp_details","TARGET_PROJECT_NAME":"data-engineering-gcp-practice","SOURCE_PROJECT_NAME":"data-engineering-gcp-practice","BACKUP_DATASET":"Example_dataset"}'
        data_json = json.loads(data)
        print("data_json ::", data_json)
        QUERY = ''
        result_list = []
        ENTITY_TYPE = data_json["ENTITY_TYPE"]
        DATASET_NAME = data_json["DATASET_NAME"]
        TARGET_DATASET_NAME = data_json["TARGET_DATASET_NAME"]
        ENTITY_NAME = data_json["ENTITY_NAME"]
        SOURCE_PROJECT_NAME = data_json["SOURCE_PROJECT_NAME"]
        TARGET_PROJECT_NAME = data_json["TARGET_PROJECT_NAME"]
        BACKUP_DATASET = data_json["BACKUP_DATASET"]
        print("Request values::ENTITY_TYPE=", ENTITY_TYPE, " ... DATASET_NAME=", DATASET_NAME, " ...ENTITY_NAME=",ENTITY_NAME)

        SQL_QUERY = ''
        """Source"""
        if (ENTITY_TYPE.__eq__('SP')):
            QUERY = CREATE_SP.format(SOURCE_PROJECT_NAME=SOURCE_PROJECT_NAME, DATASET=DATASET_NAME ,TABLE_NAME=ENTITY_NAME)
        elif (ENTITY_TYPE.__eq__('TABLE')):
            QUERY = CREATE_TABLE.format(SOURCE_PROJECT_NAME=SOURCE_PROJECT_NAME,DATASET=DATASET_NAME ,TABLE_NAME=ENTITY_NAME)           
        elif (ENTITY_TYPE.__eq__('VIEW')):
            QUERY = CREATE_VIEW.format(SOURCE_PROJECT_NAME=SOURCE_PROJECT_NAME,DATASET=DATASET_NAME ,TABLE_NAME=ENTITY_NAME)
        else:
            print("  ENTITY NOT FOUND In Source  ")
            
        rows = execute_query(QUERY)
        for row in rows:
            SQL_QUERY = row[-1]
        print("*****************Source table***************")    
        print("SQL_QUERY :::",SQL_QUERY)
        

        
        """Target
        check table exsist in destination and take backup (master entity)"""
        if (ENTITY_TYPE.__eq__('TABLE')):
            QUERY1 = CREATE_TABLE_TAR.format(TARGET_PROJECT_NAME=TARGET_PROJECT_NAME,DATASET=TARGET_DATASET_NAME ,TABLE_NAME=ENTITY_NAME)            
        else:
            print("  ENTITY NOT FOUND In Target  ")  
        SQL_QUERY1 = ''
        rows = execute_query(QUERY1)
        
        for row in rows:
            print(row)
            SQL_QUERY1 = row[-1]
        print(type(SQL_QUERY1))
        print("*****************target table***************")   
        print("SQL_QUERY1 :::",SQL_QUERY1)
        if  SQL_QUERY1:           
            if (ENTITY_TYPE.__eq__('TABLE')):               
                deploy_ddl('CREATE OR REPLACE TABLE `data-engineering-gcp-practice.Example_dataset1.Emp_details_backup` as SELECT * FROM `data-engineering-gcp-practice.Example_dataset1.Emp_details`;')
                
        
        
        
        if (ENTITY_TYPE.__eq__('SP')):
            SQL_QUERY = SQL_QUERY.replace('CREATE PROCEDURE `' + SOURCE_PROJECT_NAME + '`.' + DATASET_NAME + '.' + ENTITY_NAME + '()','CREATE OR REPLACE PROCEDURE `' + TARGET_PROJECT_NAME + '.' + TARGET_DATASET_NAME + '.' + ENTITY_NAME + '`()')
            create(ENTITY_NAME,SQL_QUERY)
        elif (ENTITY_TYPE.__eq__('TABLE')):             
            SQL_QUERY = SQL_QUERY.replace('CREATE TABLE `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '`','CREATE OR REPLACE TABLE `' + TARGET_PROJECT_NAME + '.' + TARGET_DATASET_NAME + '.' + ENTITY_NAME + '`')
            create(ENTITY_NAME,SQL_QUERY)
        elif (ENTITY_TYPE.__eq__('VIEW')):
            #print("view=============",'CREATE VIEW `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '` AS') 
            SQL_QUERY = SQL_QUERY.replace('CREATE VIEW `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '`','CREATE OR REPLACE VIEW `' + TARGET_PROJECT_NAME + '.' + TARGET_DATASET_NAME + '.' + ENTITY_NAME + '`')
            #SQL_QUERY = SQL_QUERY.replace('CREATE VIEW `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '` AS','CREATE OR REPLACE VIEW `'+TARGET_PROJECT_NAME+ '.' + TARGET_DATASET_NAME+ '.' +ENTITY_NAME+ '` AS')
            #print("view after...",'CREATE OR REPLACE VIEW `'+TARGET_PROJECT_NAME+ '.' + TARGET_DATASET_NAME+ '.' +ENTITY_NAME+ '` AS')
            #print("VIEW SQL_QUERY:::",SQL_QUERY)
            create(ENTITY_NAME,SQL_QUERY)
        else:
            raise Exception("ENTITY TYPE NOT FOUND")
        print("*****************after***************")
        #print("SQL_QUERY :::",SQL_QUERY)    
        deploy_ddl(SQL_QUERY)
        print("Successfully deployed DDL in target...")
        
    except Exception as e:
        logger.error(" ENTITY NOT FOUND  {}".format(e))  

hello_world()        