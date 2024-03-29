import glob
import shutil
from loguru import logger
import pandas as pd
import sqlite3
from pyparsing import Optional
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

from lib.etl_constants import *

from .etl_util import check_mandatory_proeperties, decrpyt_password, get_number_of_lines, process_template, run_subprocess_command
from .etl_context import context_dict
import subprocess
from pathlib import Path
import os

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
    if "REPLACEME" in connection_string or '{dbid}_{env}_dbpassword' not in context_dict:
        decryptedpassword=decrpyt_password(context_dict.app_cfg[f'{dbid}_{env}'].password_command)
        context_dict[f'{dbid}_{env}_dbpassword'] = decryptedpassword
        #ic(context_dict.app_cfg[f'{dbid}_{env}_dbpassword'])
    context_dict.app_cfg[f'{dbid}_{env}'].vendor.connection_string = connection_string.replace("REPLACEME", decryptedpassword)




#def run_bcp(dbid, env, tablename, filename, bcp_direction, first_row=None, delimiter='\",\"', last_row=None, format_file=None, jobstepid='xxxx'):
def run_bcp( jobstepid=None):
    

    step_info = context_dict.job_steps_dict[jobstepid]
    table_name = step_info.table_name
    if 'output_file' in step_info:
        bcp_direction = 'out'
        file_step_info = step_info.output_file
    else:
        bcp_direction = 'in'
        file_step_info = step_info.input_file
    filename = file_step_info.FULLFILENAME


    first_row = None
    last_row = None
    delimiter = None
    quote_char = None
    escape_char = None
    format_file = None

    if 'delimiter' in file_step_info:
        delimiter = file_step_info.delimiter
    else:
        delimiter = ','
    if 'quote_char' in file_step_info:
        quote_char = file_step_info.quote_char
    else:
        quote_char = '"'
    if 'escape_char' in file_step_info:
        escape_char = file_step_info.escape_char
    else:
        escape_char = '\\'

    if 'first_row' in file_step_info:
        first_row = file_step_info.first_row
    if 'last_row' in file_step_info:
        last_row = file_step_info.last_row

    if 'format_file' in file_step_info:
        format_file = file_step_info.format_file

    # Build the BCP command

    bcperrorlogfilename = touch_bcp_errorlog_file(f'{context_dict.job_id}_{jobstepid}')
    ic(bcperrorlogfilename)

    dbserver = context_dict.app_cfg[f'{step_info.step_database}_{step_info.env}'].dbserver
    dbinstance = context_dict.app_cfg[f'{step_info.step_database}_{step_info.env}'].dbinstance
    dbport = context_dict.app_cfg[f'{step_info.step_database}_{step_info.env}'].dbport
    dbuser = context_dict.app_cfg[f'{step_info.step_database}_{step_info.env}'].dbuser
    dbpassword = context_dict[f'{step_info.step_database}_{step_info.env}_dbpassword']
    #serverhostandport = f'{dbserver},{dbport}'
    instsep = '\\'
    defaullt_database = ""
    if table_name.count('.') != 2:
        print("Table name contains two occurrences of dot character")
        basedatabase = context_dict.app_cfg[f'{step_info.step_database}_{step_info.env}'].basedatabase
        defaullt_database  = f" -d '{basedatabase}'"
    


    
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
        

    bcp_command = f"bcp '{table_name}' out {filename} -S '{dbserver}\{dbinstance}'  -U {dbuser} -P '{dbpassword}' {defaullt_database} {first_row_command} {last_row_command} {format_file_command} {delimiter_command} -b 20000 -c -u -e {bcperrorlogfilename}"
    bcp_command_in_log = f"bcp '{table_name}' out {filename} -S '{dbserver}\{dbinstance}' -U {dbuser} -P 'PassWordHidden'  {defaullt_database} {first_row_command} {last_row_command} {format_file_command} {delimiter_command} -b 20000 -c -u -e {bcperrorlogfilename}"
    #bcp_command = f"bcp '{table_name}' out {filename} -S '{dbserver},{dbport}' -U {dbuser} -P '{dbpassword}' -b 20000 -c -u"

    ic(bcp_command)
    output_str = run_subprocess_command(bcp_command,check_error=False,check_status=False,show_command=bcp_command_in_log)

    if "ERROR" in output_str.upper():
        logger.error("BCP Produced error, please check bcp log file")

    # Check for errors in bcperrorlogfilename content
    with open(bcperrorlogfilename, 'r') as file:
        content = file.read()
        if "ERROR" in content.upper():
            logger.error("BCP Produced error, please check log file")
        if content:
            logger.info(content)

    if os.path.exists(bcperrorlogfilename):
        os.remove(bcperrorlogfilename)

    ic('examin outputstr')
    ic(output_str.splitlines()[-3:])

    if output_str and len(output_str.splitlines()) > 3:
        logger.info('Trying to get BCP info')
        if 'rows copied' in output_str.splitlines()[-3]:
            bcp_output_count = int(output_str.splitlines()[-3].split()[0])
            logger.info(f"BCP Row Count is {bcp_output_count}")
            context_dict.job_steps_dict[jobstepid].BCPPROCROWCNT = bcp_output_count

        if 'packet size' in output_str.splitlines()[-2]:
            packet_size = int(output_str.splitlines()[-2].split(':')[1].strip())
            logger.info(f"Network packet size (bytes) {packet_size}")
        
        if 'Clock Time' in output_str.splitlines()[-1]:
            clock_time_ms = int(output_str.splitlines()[-1].split('Average')[0].split(':')[1].strip())
            logger.info(f"BCP Clock Time (ms) {clock_time_ms}")

        if 'Average' in output_str.splitlines()[-1]:
            avg_rows_per_second = float(output_str.splitlines()[-1].split('rows per sec.')[0].split(': (')[1].strip())
            logger.info(f"Averarge Rows Processed per sec {avg_rows_per_second}")
    
    #sample newworkrecord line as follows

    if os.path.exists(filename):
        if  bcp_direction == 'out' and quote_char is not None:
            sed_command = f"sed -i.bak 's/^..*$/{quote_char}&{quote_char}/' {filename}"
            run_subprocess_command(sed_command)
            if os.path.exists(f"{filename}.bak"):
                os.remove(f"{filename}.bak")



    
    


def touch_bcp_errorlog_file(job_and_step):
    filename = context_dict.app_cfg.logpath / f'bcp_error_{job_and_step}.txt'
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'w').close()
    return filename






@logger.catch(reraise=True)
def table_bcp_extractor_runner(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    check_mandatory_proeperties(step_info,MANDATORY_TABLE_BCP_EXTRACTOR_PROPERTIES)
    table_name = step_info.table_name
    step_database = step_info.step_database
    process_connecton_string(step_database,step_info.env)
    generate_complete_filename(step_info.output_file)
    if 'headers_in_order' in step_info.output_file:
        step_info.output_file.headers_in_order = list(map(process_template,step_info.output_file.headers_in_order))
        ic(step_info.output_file.headers_in_order)
    if 'ext' not in step_info.output_file:
        step_info.output_file.ext = ''
    run_bcp(jobstepid=jobstepid)
    process_file_counts(step_info.output_file)
    if 'trailers_in_order' in step_info.output_file:
        step_info.output_file.trailers_in_order = list(map(process_template,step_info.output_file.trailers_in_order))
        ic(step_info.output_file.headers_in_order)
    if not os.path.exists(step_info.output_file.FULLFILENAME):
        logger.error(f"BCP process failed, File {step_info.output_file.FULLFILENAME} does not exist")
        raise FileNotFoundError(f"BCP process failed, File {step_info.output_file.FULLFILENAME} does not exist")
    add_header_trailer(step_info.output_file.FULLFILENAME, step_info.output_file.headers_in_order, step_info.output_file.trailers_in_order)
    if 'validation' in step_info and step_info.validation and step_info.validation == 'Y':
        step_info.output_file.FILELINECOUNT = get_number_of_lines(step_info.output_file.FULLFILENAME)
        if step_info.output_file.FILELINECOUNT != step_info.output_file.EXPECTEDLINECOUNT:
            logger.error(f"Validation failed fpr EXPECTEDLINECOUNT vs FILELINECOUNT, Expected {step_info.output_file.EXPECTEDLINECOUNT=} but got {step_info.output_file.FILELINECOUNT=}")
            raise ValueError(f"Validation failed for EXPECTEDLINECOUNT vs FILELINECOUNT, Expected {step_info.output_file.EXPECTEDLINECOUNT=} but got {step_info.output_file.FILELINECOUNT=}")
        else:
            logger.info(f"Validation passed for EXPECTEDLINECOUNT vs FILELINECOUNT, Expected {step_info.output_file.EXPECTEDLINECOUNT=} and got {step_info.output_file.FILELINECOUNT=}")
        if step_info.BCPPROCROWCNT != step_info.output_file.DATALINECOUNT:
            logger.error(f"Validation failed for BCPPROCROWCNT vs DATALINECOUNT, Expected {step_info.BCPPROCROWCNT=} but got {step_info.output_file.DATALINECOUNT=}")
            raise ValueError(f"Validation failed, BCPPROCROWCNT vs DATALINECOUNT, Expected {step_info.BCPPROCROWCNT=} but got {step_info.output_file.DATALINECOUNT=}")
        else:
            logger.info(f"Validation passed BCPPROCROWCNT vs DATALINECOUNT, Expected {step_info.BCPPROCROWCNT=} and got {step_info.output_file.DATALINECOUNT=}")
        
   
    



def mask_string_for_validation(string_to_mask, reference_string, char_to_mask, char_to_replace = ' '):
    mask_locations = [i for i, char in enumerate(string_to_mask) if char == char_to_mask]
    s2_len = len(reference_string)
    reference_string = list(reference_string)
    for loc in mask_locations:
        if s2_len > loc:            
            reference_string[loc] = char_to_replace
    reference_string = ''.join(reference_string)
    return reference_string


# @logger.catch(reraise=True)
# def add_header_trailer_old(file_path, header_list, trailer_list):
#     with open(file_path, 'r+') as file:
#         content = file.readlines()
#         file.seek(0, 0)
#         file.write('\n'.join(header_list) + '\n')
#         file.write(''.join(content))
#         file.write('\n'.join(trailer_list) + '\n')


# @logger.catch(reraise=True)
# def add_header_trailer_shell(file_path:str, header_list :list, trailer_list:list):
#     logger.info(f"Adding header and trailer to file {file_path}")
#     header_string = '\n'.join(header_list)
#     trailer_string = '\n'.join(trailer_list)
#     header_command = f"sed -i '1s/^/{header_string}\\n/' filename"
#     run_subprocess_command(header_command)
#     trailer_command = f"echo '{trailer_string}' >> filename"
#     run_subprocess_command(trailer_command)
#     logger.info(f"Header and trailer added to file {file_path}")

logger.catch(reraise=True)
def add_header_trailer(file_path: str, header_list: list[str] = None, trailer_list: list[str] = None):
    header = '\n'.join(header_list) + '\n' if header_list else ''
    trailer = '\n'.join(trailer_list) + '\n' if trailer_list else ''

    process_id = os.getpid()

    temp_file = f'{file_path}.tmp.{process_id}'

    with open(file_path, 'r') as read_obj, open(temp_file, 'w') as write_obj:
        write_obj.write(header)
        shutil.copyfileobj(read_obj, write_obj)
        write_obj.write(trailer)

    os.rename(temp_file, file_path)

def generate_complete_filename(file_info):
    complete_filename = Path(file_info.path) / (process_template(file_info.prefix) + file_info.ext)
    file_info.FULLFILENAME = str(complete_filename)
    file_info.FILENAME = Path(complete_filename).name
    file_info.FULLFILENAMEWE = Path(complete_filename).stem
    return str(complete_filename)


def process_file_counts(file_info):
    file_info.DATALINECOUNT = get_number_of_lines(file_info.FULLFILENAME)
    file_info.EXPECTEDLINECOUNT = file_info.DATALINECOUNT
    if HEADERS_IN_ORDER in file_info:
        file_info.EXPECTEDLINECOUNT = file_info.EXPECTEDLINECOUNT + len(file_info.headers_in_order)
    if TRAILERS_IN_ORDER in file_info:
        file_info.EXPECTEDLINECOUNT = file_info.EXPECTEDLINECOUNT + len(file_info.trailers_in_order)
    return file_info


#todo add validation flags