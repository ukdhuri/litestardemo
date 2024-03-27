import glob
import shutil
from loguru import logger
import pandas as pd
import sqlite3
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker
import pyodbc
import jaydebeapi
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
import subprocess
import os
import sqlalchemy.pool as pool
from icecream import ic

from .etl_util import decrpyt_password
from .etl_context import context_dict

def getconn(connection_string):
    driverpath='*.jar'
    jarfiles=glob.glob(driverpath)
    jarfiles = ['jconn4-16.0.jar','bcprov-jdk15on-1.70.jar']
    driver_args={'JCE_PROVIDER_CLASS':'org.bouncycastle.jce.provider.BouncyCastleProvider'}
    c = jaydebeapi.connect(jclassname='com.sybase.jdbc4.jdbc.SybDriver',url=connection_string, driver_args=driver_args, jars=jarfiles)
    return c

def get_query_data_pd(connection_string,sql_query) -> pd.DataFrame:
    if not connection_string.startswith('jdbc'):
        df = pd.read_sql_query(sql_query,sessionmaker(bind=create_engine(connection_string))().get_bind())
    else:
        df = pd.read_sql_query(sql_query,getconn(connection_string))
    return df


async def execute_query(connection_string, sql_query):
    if not connection_string.startswith('jdbc'):
        # with create_engine(connection_string).connect() as conn:
        #     conn.execute(sql_query)
        await execute_query_fornative(connection_string, sql_query)
    else:
        with getconn(connection_string) as conn:
            conn.cursor().execute(sql_query)


async def execute_query_fornative(connection_string, sql_query):
    connection_string = connection_string.replace('pyodbc','aioodbc').replace('pymysql','aiomysql')
    async with create_async_engine(connection_string) as engine:
        async with engine.begin() as conn:
            try:
                await conn.execute(sql_query)
                await conn.commit()
            except:
                await conn.rollback()
                raise



def process_connecton_string(dbid,env):
    connection_string = context_dict.app_cfg[f'{dbid}_{env}'].vendor.connection_string
    if "REPLACEME" in connection_string:
        decryptedpassword=decrpyt_password(context_dict.app_cfg[f'{dbid}_{env}'].password_command)
        context_dict[f'{dbid}_{env}_dbpassword'] = decryptedpassword
        #ic(context_dict.app_cfg[f'{dbid}_{env}_dbpassword'])
    context_dict.app_cfg[f'{dbid}_{env}'].vendor.connection_string = connection_string.replace("REPLACEME", decryptedpassword)




#def run_bcp(dbid, env, tablename, filename, bcp_direction, first_row=None, delimiter='\",\"', last_row=None, format_file=None, jobstepid='xxxx'):
def run_bcp(dbid, env, tablename, filename, bcp_direction, first_row=None, delimiter=',',quote_char='"', last_row=None, format_file=None, jobstepid='xxxx'):

    # Build the BCP command

    bcperrorlogfilename = touch_bcp_errorlog_file(jobstepid)

    dbserver = context_dict.app_cfg[f'{dbid}_{env}'].dbserver
    dbinstance = context_dict.app_cfg[f'{dbid}_{env}'].dbinstance
    dbport = context_dict.app_cfg[f'{dbid}_{env}'].dbport
    dbuser = context_dict.app_cfg[f'{dbid}_{env}'].dbuser
    dbpassword = context_dict[f'{dbid}_{env}_dbpassword']
    #serverhostandport = f'{dbserver},{dbport}'
    instsep = '\\'
    bcp_server = f"{dbserver}{instsep}{dbinstance}"
    basedatabase = context_dict.app_cfg[f'{dbid}_{env}'].basedatabase


    command = ['bcp', tablename, bcp_direction, filename, '-S', bcp_server, '-U', dbuser, '-P', dbpassword, '-c' , '-u']
    
    # Add optional arguments if provided
    if first_row is not None and bcp_direction == 'in':
        first_row_command = f'-F {first_row}'
    else:
        first_row_command = ''

    if last_row is not None and bcp_direction == 'in':
        last_row_command = f'-L {last_row}'
    else:
        last_row_command = ''

    if delimiter is not None:
        if quote_char is not None and bcp_direction == 'out':
            delimiter = f'{quote_char}{delimiter}{quote_char}'
        delimiter_command = f"-t '{delimiter}'"
    else:
        delimiter_command = ''

    if format_file is not None:
        format_file_command = f'-f {format_file}'
    else:
        format_file_command = ''







    bcp_command = f"bcp '{tablename}' out {filename} -S '{dbserver}\{dbinstance}' -U {dbuser} -P '{dbpassword}' {first_row_command} {last_row_command} {format_file_command} {delimiter_command} -b 20000 -c -u -e {bcperrorlogfilename}"
    #bcp_command = f"bcp '{tablename}' out {filename} -S '{dbserver},{dbport}' -U {dbuser} -P '{dbpassword}' -b 20000 -c -u"

    ic(bcp_command)


    result = subprocess.run(bcp_command, shell=True, capture_output=True, text=True)
    output_str = result.stdout.strip()
    ic(output_str)

    if "ERROR" in output_str.upper():
        logger.error("BCP Produced error, please check bcp log file")
    if output_str:
        logger.info(output_str)


    # Check for errors in bcperrorlogfilename content
    with open(bcperrorlogfilename, 'r') as file:
        content = file.read()
        if "ERROR" in content.upper():
            logger.error("BCP Produced error, please check log file")
        if content:
            logger.info(content)

    if os.path.exists(bcperrorlogfilename):
        os.remove(bcperrorlogfilename)

    if os.path.exists(filename):
        if  bcp_direction == 'out' and quote_char is not None:
            sed_command = f"sed -i.bak 's/^..*$/{quote_char}&{quote_char}/' {filename}"
            result = subprocess.run(sed_command, shell=True, capture_output=True, text=True)
            output_str = result.stdout.strip()
            logger.info(output_str)


def touch_bcp_errorlog_file(jobstepid):
    filename = context_dict.app_cfg.logpath / f'bcp_error_{jobstepid}.txt'
    if os.path.exists(jobstepid):
        os.remove(jobstepid)
    open(jobstepid, 'w').close()
    return filename






@logger.catch(reraise=True)
def table_bcp_extractor_runner(jobstepid,env):
    step_info = context_dict.job_steps_dict[jobstepid]
    table_name = step_info.table_name
    step_database = step_info.step_database
    process_connecton_string(step_database,env)
    run_bcp(step_database, env, table_name, 'aaa.txt', 'out')


def mask_string_for_validation(string_to_mask, reference_string, char_to_mask, char_to_replace = ' '):
    mask_locations = [i for i, char in enumerate(string_to_mask) if char == char_to_mask]
    s2_len = len(reference_string)
    reference_string = list(reference_string)
    for loc in mask_locations:
        if s2_len > loc:            
            reference_string[loc] = char_to_replace
    reference_string = ''.join(reference_string)
    return reference_string