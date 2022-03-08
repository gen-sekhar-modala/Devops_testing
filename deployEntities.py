from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import base64
import ast, os
import logging as logger
import time
import traceback
import json
import datetime
from flask import request

CREATE_TABLE = (
    "SELECT * FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.TABLES` WHERE table_name= '{TABLE_NAME}' ;")
CREATE_SP = ("SELECT * FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.ROUTINES` WHERE routine_name= '{TABLE_NAME}';")
#CREATE_VIEW = ("SELECT table_catalog,table_schema,    table_name,    view_definition  FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.VIEWS` WHERE table_name= '{TABLE_NAME}';")
CREATE_VIEW = ("SELECT *  FROM  `{SOURCE_PROJECT_NAME}.{DATASET}.INFORMATION_SCHEMA.TABLES` WHERE table_name= '{TABLE_NAME}';")


def execute_query(QUERY):
    try:
        client = bigquery.Client()
        query_job = client.query(QUERY)
        print(query_job)
        print("Execute_query......")
        rows = query_job.result()
        print(row)
    except:
        print("Error while getting information query ....")
        raise
    return rows


def deploy_ddl(sql):
    try:
        result_list = []
        SQL_QUERY = ''
        print("deploy_ddl....")
        client = bigquery.Client()
        job = client.query(sql)
        job.result()
    except:
        print("Error while deploying DDL ....")
        raise


def hello_world():
    try:  
#        data = request.data
#        data_json = json.loads(data)
#        print("data_json ::", data_json)
#        QUERY = ''
#        result_list = []
#        ENTITY_TYPE = data_json["ENTITY_TYPE"]
#        DATASET_NAME = data_json["DATASET_NAME"]
#        TARGET_DATASET_NAME = data_json["TARGET_DATASET_NAME"]
#        ENTITY_NAME = data_json["ENTITY_NAME"]
#        SOURCE_PROJECT_NAME = data_json["SOURCE_PROJECT_NAME"]
#        TARGET_PROJECT_NAME = data_json["TARGET_PROJECT_NAME"]
        #import os
        ENTITY_TYPE=os.environ.get("ENTITY_TYPE")
        DATASET_NAME=os.environ.get("DATASET_NAME")
        TARGET_DATASET_NAME=os.environ.get("TARGET_DATASET_NAME")
        ENTITY_NAME=os.environ.get("ENTITY_NAME")
        TARGET_PROJECT_NAME=os.environ.get("TARGET_PROJECT_NAME")
        SOURCE_PROJECT_NAME=os.environ.get("SOURCE_PROJECT_NAME")  
        print("Request values::ENTITY_TYPE=", ENTITY_TYPE, " ... DATASET_NAME=", DATASET_NAME, " ...ENTITY_NAME=",ENTITY_NAME)

        SQL_QUERY = ''
        if (ENTITY_TYPE.__eq__('SP')):
            QUERY = CREATE_SP.format(SOURCE_PROJECT_NAME=SOURCE_PROJECT_NAME, DATASET=DATASET_NAME ,TABLE_NAME=ENTITY_NAME)
        elif (ENTITY_TYPE.__eq__('TABLE')):
            QUERY = CREATE_TABLE.format(SOURCE_PROJECT_NAME=SOURCE_PROJECT_NAME,DATASET=DATASET_NAME ,TABLE_NAME=ENTITY_NAME)
        elif (ENTITY_TYPE.__eq__('VIEW')):
            QUERY = CREATE_VIEW.format(SOURCE_PROJECT_NAME=SOURCE_PROJECT_NAME,DATASET=DATASET_NAME ,TABLE_NAME=ENTITY_NAME)            
        else:
            print("  ENTITY NOT FOUND   ")

        rows = execute_query(QUERY)
        for row in rows:
            SQL_QUERY = row[-1]

        print("SQL_QUERY :::",SQL_QUERY)
        if (ENTITY_TYPE.__eq__('SP')):
            SQL_QUERY = SQL_QUERY.replace('CREATE PROCEDURE `' + SOURCE_PROJECT_NAME + '`.' + DATASET_NAME + '.' + ENTITY_NAME + '()','CREATE OR REPLACE PROCEDURE `' + TARGET_PROJECT_NAME + '.' + TARGET_DATASET_NAME + '.' + ENTITY_NAME + '`()')
        elif (ENTITY_TYPE.__eq__('TABLE')):
            SQL_QUERY = SQL_QUERY.replace('CREATE TABLE `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '`','CREATE OR REPLACE TABLE `' + TARGET_PROJECT_NAME + '.' + TARGET_DATASET_NAME + '.' + ENTITY_NAME + '`')
        elif (ENTITY_TYPE.__eq__('VIEW')):
            print("view=============",'CREATE VIEW `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '` AS') 
            SQL_QUERY = SQL_QUERY.replace('CREATE VIEW `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '`','CREATE OR REPLACE VIEW `' + TARGET_PROJECT_NAME + '.' + TARGET_DATASET_NAME + '.' + ENTITY_NAME + '`')
            #SQL_QUERY = SQL_QUERY.replace('CREATE VIEW `' + SOURCE_PROJECT_NAME + '.' + DATASET_NAME + '.' + ENTITY_NAME + '` AS','CREATE OR REPLACE VIEW `'+TARGET_PROJECT_NAME+ '.' + TARGET_DATASET_NAME+ '.' +ENTITY_NAME+ '` AS')
            print("view after...",'CREATE OR REPLACE VIEW `'+TARGET_PROJECT_NAME+ '.' + TARGET_DATASET_NAME+ '.' +ENTITY_NAME+ '` AS')
        #SQL_QUERY=SQL_QUERY
            print("VIEW SQL_QUERY:::",SQL_QUERY)
        else:
            raise Exception("ENTITY TYPE NOT FOUND")

        print("SQL_QUERY :::",SQL_QUERY)    
        deploy_ddl(SQL_QUERY)
        print("Successfully deployed DDL in target...")
        print(SQL_QUERY)
    except Exception as e:
        logger.error(" ENTITY NOT FOUND  {}".format(e)) 
        

        
print("Started...")
hello_world()
print("Completed function....")
