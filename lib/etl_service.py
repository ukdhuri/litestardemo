import csv
import glob
import math
import shutil
from typing import Union
from adbc_driver_manager import IntegrityError
from benedict import benedict
from loguru import logger
import pandas as pd
import sqlite3
from pyparsing import Optional
from sqlalchemy import Connection, Engine, MetaData, Table, create_engine, text
from sqlalchemy.orm import sessionmaker
import pyodbc
import jaydebeapi
import asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker,create_async_engine
import subprocess
import os
import sqlalchemy.pool as pool
from icecream import ic
from lib.etl_constants import *
from .etl_util import check_and_delete_file, check_mandatory_proeperties, decrpyt_password, format_row, get_last_n_lines, get_n_lines_from_top, get_number_of_lines, list_to_string, process_template, read_yaml_file, run_subprocess_command, read_sql_file, string_to_list
from .etl_context import context_dict
import subprocess
from pathlib import Path
import os
from jinja2 import Environment, FileSystemLoader

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

def get_query_data_pd_bydbid(dbidwithenv,sql_query) -> pd.DataFrame:
    connection_string = context_dict.app_cfg[f'{dbidwithenv}'].vendor.connection_string
    if not connection_string.startswith('jdbc'):
        df = pd.read_sql_query(sql_query,sessionmaker(bind=create_engine(connection_string))().get_bind())
    else:
        df = pd.read_sql_query(sql_query,getconn(connection_string))
    return df



def get_panda_con(dbidwithenv) -> Union[Engine, Connection]:
    connection_string = context_dict.app_cfg[f'{dbidwithenv}'].vendor.connection_string
    if not connection_string.startswith('jdbc'):
        return sessionmaker(bind=create_engine(connection_string))().get_bind()
    else:
        return getconn(connection_string)

async def execute_query(connection_string, sql_query):
    if not connection_string.startswith('jdbc'):
        # with create_engine(connection_string).connect() as conn:
        #     conn.execute(sql_query)
        await execute_query_fornative(connection_string, sql_query)
    else:
        with getconn(connection_string) as conn:
            conn.cursor().execute(sql_query)

# async def execute_query_fornative(connection_string, sql_query):
#     connection_string = connection_string.replace('pyodbc','aioodbc').replace('pymysql','aiomysql')
#     engine = create_async_engine(connection_string)
#     async with engine.begin() as conn:
#         try:
#             result = await conn.execute(text(sql_query))
#             await conn.commit()
#             return result
#         except:
#             await conn.rollback()
#             raise

# async def execute_query_fornative(connection_string, sql_query):
#     connection_string = connection_string.replace('pyodbc','aioodbc').replace('pymysql','aiomysql')
#     sessionmaker = async_sessionmaker(expire_on_commit=False)
#     engine = create_async_engine(connection_string)
#     async with engine.begin() as conn:
#         try:
#             result = await conn.execute(text(sql_query))
#             await conn.commit()
#             return result
#         except:
#             await conn.rollback()
#             raise

@logger.catch(reraise=True)
async def execute_query_fornative(connection_string, sql_query):
    connection_string = connection_string.replace('pyodbc','aioodbc').replace('pymysql','aiomysql')
    asyncengine = create_async_engine(connection_string)
    sessionmaker = async_sessionmaker(expire_on_commit=False)
    async with sessionmaker(bind=asyncengine) as session1:
        try:
            async with session1.begin():
                result = await session1.execute(text(sql_query))
                await session1.close()
                return result
        except IntegrityError as exc:
            raise exc from exc







def is_special_character(char):
    """
    Checks if a character is a special character used in Perl expressions.
    Args:
        char (str): The input character to check.
    Returns:
        bool: True if the character is special and should be escaped, False otherwise.
    """
    special_characters = r'\\$@&.*?()[]{}|+^'
    return char in special_characters


def process_connecton_string(dbobject,jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    dbid = dbobject.database
    if not 'env' in dbobject:
        dbobject.env = step_info.env
    env  = dbobject.env
    connection_string = context_dict.app_cfg[f'{dbid}_{env}'].vendor.connection_string
    if "REPLACEME" in connection_string or '{dbid}_{env}_dbpassword' not in context_dict:
        decryptedpassword=decrpyt_password(context_dict.app_cfg[f'{dbid}_{env}'].password_command)
        context_dict[f'{dbid}_{env}_dbpassword'] = decryptedpassword
        ##ic(context_dict.app_cfg[f'{dbid}_{env}_dbpassword'])
    context_dict.app_cfg[f'{dbid}_{env}'].vendor.connection_string = connection_string.replace("REPLACEME", decryptedpassword)
    if 'local_context_dict' not in dbobject:
        dbobject.local_context_dict = benedict()
    dbobject.local_context_dict.connection_string = context_dict.app_cfg[f'{dbid}_{env}'].vendor.connection_string

def set_local_dbvarialbes(dbobject,jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    if not 'env' in dbobject:
        dbobject.env = step_info.env
    if 'local_context_dict' not in dbobject:
        dbobject.local_context_dict = benedict()
    dbkey = f'{dbobject.database}_{dbobject.env}'
    dbobject.local_context_dict.dbserver = context_dict.app_cfg[f'{dbkey}'].dbserver
    dbobject.local_context_dict.dbinstance = context_dict.app_cfg[f'{dbkey}'].dbinstance
    dbobject.local_context_dict.dbport = context_dict.app_cfg[f'{dbkey}'].dbport
    dbobject.local_context_dict.dbuser = context_dict.app_cfg[f'{dbkey}'].dbuser
    dbobject.local_context_dict.dbpassword = context_dict[f'{dbkey}_dbpassword']
    dbobject.local_context_dict.dialect = context_dict.app_cfg[f'{dbkey}'].vendor.dialect
    dbobject.local_context_dict.basedatabase = context_dict.app_cfg[f'{dbkey}'].basedatabase
    if dbobject.local_context_dict.dialect == 'tsql':
        dbobject.local_context_dict.dbschema = 'dbo'


def set_local_apivariables(apiobject,jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    if not 'env' in apiobject:
        apiobject.env = step_info.env
    if 'local_context_dict' not in apiobject:
        apiobject.local_context_dict = benedict()
    apikey = f'{apiobject.name}_{apiobject.env}'
    apiobject.local_context_dict.api_url = context_dict.app_cfg[f'{apikey}'].api_url



    # dbobject.local_context_dict.dbinstance = context_dict.app_cfg[f'{dbkey}'].dbinstance
    # dbobject.local_context_dict.dbport = context_dict.app_cfg[f'{dbkey}'].dbport
    # dbobject.local_context_dict.dbuser = context_dict.app_cfg[f'{dbkey}'].dbuser
    # dbobject.local_context_dict.dbpassword = context_dict[f'{dbkey}_dbpassword']
    # dbobject.local_context_dict.dialect = context_dict.app_cfg[f'{dbkey}'].vendor.dialect
    # dbobject.local_context_dict.basedatabase = context_dict.app_cfg[f'{dbkey}'].basedatabase
    # if dbobject.local_context_dict.dialect == 'tsql':
    #     dbobject.local_context_dict.dbschema = 'dbo'





#def run_bcp(dbid, env, tablename, filename, bcp_direction, first_row=None, delimiter='\",\"', last_row=None, format_file=None, jobstepid='xxxx'):
def run_bcp(jobstepid=None):
    step_info = context_dict.job_steps_dict[jobstepid]

    if context_dict.job_steps_dict[jobstepid].type == SQL_BCP_EXTRACTOR or context_dict.job_steps_dict[jobstepid].type == TABLE_BCP_EXTRACTOR:
        bcp_direction = 'out'
    else:
        bcp_direction = 'in'
    file_step_info = context_dict.job_steps_dict[jobstepid].file
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
    if 'escape_char' in file_step_info:
        escape_char = file_step_info.escape_char
    else:
        escape_char = '\\'

    if bcp_direction == 'in':
        if 'first_row' in file_step_info:
            first_row = len(file_step_info.headers_in_order) + 1
        if 'last_row' in file_step_info:
            last_row = file_step_info.FILELINECOUNT - len(file_step_info.trailers_in_order)

    if 'format_file' in file_step_info:
        format_file = file_step_info.format_file

    defaullt_database = ""
    bcp_source = ""

    if step_info.type == SQL_BCP_EXTRACTOR:
        #set_local_dbvarialbes(step_info.sqlfile,jobstepid)
        accesstype='sqlfile'
        bcp_sql_query = read_sql_file(f'sql/{step_info.sqlfile.name}')
        bcp_source = bcp_sql_query
        bcp_direction = 'queryout'
        defaullt_database  = f" -d '{step_info.sqlfile.local_context_dict.basedatabase}'"

    else:
        #set_local_dbvarialbes(step_info.table,jobstepid)
        accesstype='table'
        bcp_source = step_info.table.name
        if step_info.table.name.count('.') != 2:
            print("Table name contains two occurrences of dot character")
            defaullt_database  = f" -d '{step_info.table.local_context_dict.basedatabase}'"

    bcperrorlogfilename = touch_bcp_errorlog_file(f'{context_dict.job_id}_{jobstepid}')

    # Add optional arguments if provided
    if first_row is not None and bcp_direction == 'in':
        first_row_command = f'-F {first_row}'
    else:
        first_row_command = ''

    if last_row is not None and bcp_direction == 'in':
        last_row_command = f'-L {last_row}'
    else:
        last_row_command = ''

    if format_file is not None:
        format_file_command = f'-f {format_file}'
    else:
        format_file_command = ''
    bcpfile = filename

    clean_file_used = False
    if bcp_direction == 'in':
        clean_file_used = False
        if quote_char is not None:
            check_and_delete_file(f"{filename}.cleaned")
            logger.info("Cleaning Source File to remove quotes and replace delimter with stard unit seperator")
            logger.info(f"replacing delimiter {delimiter} with ␟")
            delimer_to_be_replaced = delimiter
            if is_special_character(delimiter):
                if len(delimiter) < 2:
                    delimer_to_be_replaced = '\\' + delimiter
            escape_char_perl = escape_char
            if is_special_character(escape_char_perl):
                if len(escape_char_perl) < 2:
                    escape_char_perl = '\\' + escape_char_perl
            braket_start = '{'
            braket_end = '}'
            perl_command = f"perl -p -e 's{braket_start}(?<!{escape_char_perl}){delimer_to_be_replaced}{braket_end}{braket_start}␟{braket_end}g' {filename} > {filename}.cleaned"
            delimiter = f'␟'
            print(perl_command)
            #perl_command = f"perl -pe 's/{delimter_to_replace}/␟/g' {filename} > {filename}.cleaned"
            #sed_command = f"sed 's/[^{escape_char}]{delimiter}/,/g' {filename} > {filename}.cleaned"
            run_subprocess_command(perl_command)
            logger.info(f"replacing quote_char {quote_char} with empty string")
            quote_char_perl = quote_char
            if is_special_character(quote_char_perl):
                if len(quote_char_perl) < 2:
                    quote_char_perl = '\\' + quote_char_perl
            perl_command = f"perl -pi -e 's{braket_start}(?<!{escape_char_perl}){quote_char_perl}{braket_end}{braket_start}{braket_end}g' {filename}.cleaned"
            #sed_command = f"sed -i 's/[^{escape_char}]{quote_char}//g' {filename}.cleaned"
            run_subprocess_command(perl_command)
            clean_file_used = True
        if 'headers_in_order' in step_info.file and step_info.file.headers_in_order: 
            if clean_file_used:
                remove_header_command = f"sed -i '1,{len(file_step_info.headers_in_order)}d' {filename}.cleaned"
            else:
                remove_header_command = f"sed '1,{len(file_step_info.headers_in_order)}d' {filename} > {filename}.cleaned"
            ic(remove_header_command)
            run_subprocess_command(remove_header_command)
            clean_file_used = True
        if 'trailers_in_order' in step_info.file and step_info.file.trailers_in_order:
            header_len  = 0
            if 'headers_in_order' in step_info.file and step_info.file.headers_in_order:
                header_len = len(file_step_info.headers_in_order)
            tailer_var = file_step_info.FILELINECOUNT - len(file_step_info.trailers_in_order) + 1 - header_len
            if clean_file_used:
                remove_trailer_command = f"sed -i '{tailer_var},$d' {filename}.cleaned"
            else:
                remove_trailer_command = f"sed '{tailer_var},$d' {filename} > {filename}.cleaned"
            print(remove_trailer_command)
            run_subprocess_command(remove_trailer_command)
            clean_file_used = True
        # sed_command = f"sed -i 's/^{quote_char}//' {filename}.cleaned"
        # run_subprocess_command(sed_command)
    if clean_file_used:
        bcpfile = f"{filename}.cleaned"
    else:
        bcpfile = filename

    if quote_char is not None and ( bcp_direction == 'out' or bcp_direction == 'queryout' ):
        delimiter = f'{quote_char}{delimiter}{quote_char}'
    delimiter_command = f"-t '{delimiter}'"  

    if bcp_direction == 'out' or bcp_direction == 'queryout':
        check_and_delete_file(f"{bcpfile}")

    bcp_command = f"bcp '{bcp_source}' {bcp_direction} {bcpfile} -S '{step_info[accesstype].local_context_dict.dbserver}\{step_info[accesstype].local_context_dict.dbinstance}'  -U {step_info[accesstype].local_context_dict.dbuser} -P '{step_info[accesstype].local_context_dict.dbpassword}' {defaullt_database} {first_row_command} {last_row_command} {format_file_command} {delimiter_command} -b 20000 -c -u -e {bcperrorlogfilename}"
    bcp_command_in_log = f"bcp '{bcp_source}' {bcp_direction} {bcpfile} -S '{step_info[accesstype].local_context_dict.dbserver}\{step_info[accesstype].local_context_dict.dbinstance}' -U {step_info[accesstype].local_context_dict.dbuser} -P 'PassWordHidden'  {defaullt_database} {first_row_command} {last_row_command} {format_file_command} {delimiter_command} -b 20000 -c -u -e {bcperrorlogfilename}"
    #bcp_command = f"bcp '{table.name}' out {filename} -S '{dbserver},{dbport}' -U {dbuser} -P '{dbpassword}' -b 20000 -c -u"

    ic(bcp_command)
    output_str = run_subprocess_command(bcp_command,show_command=bcp_command_in_log,additional_log_files=[str(bcperrorlogfilename)])
    check_and_delete_file(f"{filename}.cleaned")
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
    if output_str and len(output_str.splitlines()) > 3:
        logger.info('Trying to get BCP info')
        if 'rows copied' in output_str.splitlines()[-3]:
            bcp_output_count = int(output_str.splitlines()[-3].split()[0])
            logger.info(f"BCP Row Count is {bcp_output_count}")
            context_dict.job_steps_dict[jobstepid].PROCESSEDROWCNT = bcp_output_count

        if 'packet size' in output_str.splitlines()[-2]:
            packet_size = int(output_str.splitlines()[-2].split(':')[1].strip())
            logger.info(f"Network packet size (bytes) {packet_size}")
        
        if 'Clock Time' in output_str.splitlines()[-1]:
            clock_time_ms = int(output_str.splitlines()[-1].split('Average')[0].split(':')[1].strip())
            logger.info(f"BCP Clock Time (ms) {clock_time_ms}")

        if 'Average' in output_str.splitlines()[-1]:
            avg_rows_per_second = float(output_str.splitlines()[-1].split('rows per sec.')[0].split(': (')[1].strip())
            logger.info(f"Averarge Rows Processed per sec {avg_rows_per_second}")

    if os.path.exists(filename):
        if  ( bcp_direction == 'out' or bcp_direction == 'queryout' ) and quote_char is not None:
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
async def  sql_bcp_extractor(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    check_mandatory_proeperties(step_info,MANDATORY_SQL_BCP_EXTRACTOR_PROPERTIES)
    await bcp_extractor_runner(jobstepid)


@logger.catch(reraise=True)
async def  cleanup_files(jobstepid):
    files = context_dict.job_steps_dict[jobstepid].files
    for file in files:
        generate_complete_filename(file)
        check_and_delete_file(file.FULLFILENAME)



@logger.catch(reraise=True)
async def  render_file(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    generate_complete_filename(step_info.filei)
    generate_complete_filename(step_info.fileo)
    await preprocess_input_file(step_info.filei)
    file_step_info_i, delimiter_i, quote_char_i, escape_char_i, widths_i, alignments_i, quoting_i, column_list_i = initilize_common_file_properties(step_info,'filei')
    #file_step_info_o, delimiter_o, quote_char_o, escape_char_o, widths_o, alignments_o, quoting_o, column_list_o = initilize_common_file_properties(step_info,'fileo')
    ingestion_file = cleanheader_and_trailer(file_step_info_i)
    chunksize = get_chunksize(step_info,'filei')
    if file_step_info_i.type == 'delimitedfile':
        df = pd.read_csv(f"{ingestion_file}", delimiter=delimiter_i, quotechar=quote_char_i,quoting=quoting_i, escapechar=escape_char_i , names=column_list_i, header=None)
    elif file_step_info_i.type == 'fixedwidthfile':
        widths = [int(width) for width in string_to_list(widths_i)]
        df = pd.read_fwf(f"{ingestion_file}", widths=widths, names=column_list_i, chunksize=chunksize, header=None, alignments=alignments_i)
    step_info.PROCESSEDROWCNT = len(df)

    envtmp = Environment(loader=FileSystemLoader('templates'))
    template = envtmp.get_template(step_info.reneder_template)
    with open(step_info.fileo.FULLFILENAME, 'w') as f:
        output  = template.render(headers=list(df.columns),rows=df.values.tolist())
        f.write(output)

   
    # Calculate the number of pages
    num_pages = math.ceil(len(df) / step_info.rows_per_page)

    # # Divide the DataFrame into chunks and write each chunk to the text file
    # with open(step_info.fileo.FULLFILENAME, 'w') as f:
    #     for page_num in range(num_pages):
    #         start = page_num * step_info.rows_per_page
    #         end = start + step_info.rows_per_page
    #         chunk = df[start:end]
    #         output = template.render(page_num=page_num+1, headers=chunk.columns.tolist(), rows=chunk.values.tolist())
    #         f.write(output)


#data = df.to_dict('records')
#rendered_template = template.render(headers=list(df.columns), rows=data)
# {% for header in headers %}{{ header }}\t{% endfor %}\n
# {% for row in rows %}{% for header in headers %}{{ row[header] }}\t{% endfor %}\n{% endfor %}
# {% for header in headers %}{{ header }}\t{% endfor %}\n
# {% for row in rows %}{% for cell in row %}{{ cell }}\t{% endfor %}\n{% endfor %}




@logger.catch(reraise=True)
async def  table_bcp_extractor(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    check_mandatory_proeperties(step_info,MANDATORY_TABLE_BCP_EXTRACTOR_PROPERTIES)
    await bcp_extractor_runner(jobstepid)


@logger.catch(reraise=True)
async def  bcp_extractor_runner(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    if step_info.type == SQL_BCP_EXTRACTOR:
        process_connecton_string(step_info.sqlfile,jobstepid)
        set_local_dbvarialbes(step_info.sqlfile,jobstepid)
    else:
        process_connecton_string(step_info.table,jobstepid)
        set_local_dbvarialbes(step_info.table,jobstepid)
    generate_complete_filename(step_info.file)
    initialize_for_outputfile(step_info)
    run_bcp(jobstepid=jobstepid)
    await process_rawoutputfile_counts(step_info.file)
    await process_output_file(step_info.file,step_info.PROCESSEDROWCNT)


@logger.catch(reraise=True)
async def table_bcp_loader(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    check_mandatory_proeperties(step_info,MANDATORY_TABLE_BCP_LOADER_PROPERTIES)
    generate_complete_filename(step_info.file)
    process_connecton_string(step_info.table,jobstepid)
    set_local_dbvarialbes(step_info.table,jobstepid)
    await preprocess_input_file(step_info.file)
    main_table_name = step_info.table.name
    table_to_bcp_into = main_table_name
    if step_info.load_type == 'truncate_and_load':
        truncate_command = f"truncate table {step_info.table.name}"
        await execute_query(step_info.table.local_context_dict.connection_string,truncate_command)
    if step_info.load_type == 'merge_and_load':
        table_to_bcp_into, local_context, column_names_list, unique_col_name_list, punique_col_name_list = await initialize_for_merge(step_info, main_table_name)
    step_info.table.name = table_to_bcp_into 
    run_bcp(jobstepid=jobstepid)
    await post_table_load_validator(jobstepid)
    if step_info.load_type == 'merge_and_load':
        await merge_table_and_clean(step_info, main_table_name, table_to_bcp_into, local_context, column_names_list, unique_col_name_list, punique_col_name_list)

async def initialize_for_merge(step_info, main_table_name):
    local_context = benedict()
    local_context = step_info.table
    drop_query = render_sql_template('drop_mtmp_tbl.sql',local_context)
    await execute_query(step_info.table.local_context_dict.connection_string,drop_query)
    clone_strucure_query = render_sql_template('clone_table_structure.sql',local_context)
    await execute_query(step_info.table.local_context_dict.connection_string,clone_strucure_query)
    all_column_names_query = render_sql_template('get_columns.sql',local_context)
    column_names_pd = get_query_data_pd_bydbid(f'{step_info.table.database}_{step_info.env}',all_column_names_query)
    column_names_list = column_names_pd['column_name'].tolist()
    ic(column_names_list)
    unique_col_name_list = []
    all_unique_columns_query = render_sql_template('get_primary_key.sql',local_context)
    uniq_col_names_pd = get_query_data_pd_bydbid(f'{step_info.table.database}_{step_info.env}',all_unique_columns_query)
    print(uniq_col_names_pd)
    punique_col_name_list = uniq_col_names_pd['column_name'].tolist()
    if 'key_columns'  in step_info.table:
        unique_col_name_list = string_to_list(step_info.table.key_columns)
    else:
        unique_col_name_list = punique_col_name_list
    ic(unique_col_name_list)
    table_to_bcp_into = f"{main_table_name}_mtmp"
    step_info.table.name = table_to_bcp_into
    return table_to_bcp_into,local_context,column_names_list,unique_col_name_list,punique_col_name_list


@logger.catch(reraise=True)
async def table_pandas_loader(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    check_mandatory_proeperties(step_info,MANDATORY_TABLE_PANDAS_LOADER_PROPERTIES)
    generate_complete_filename(step_info.file)
    process_connecton_string(step_info.table,jobstepid)
    set_local_dbvarialbes(step_info.table,jobstepid)
    await preprocess_input_file(step_info.file)
    main_table_name = step_info.table.name
    table_to_bcp_into = main_table_name
    if step_info.load_type == 'truncate_and_load':
        truncate_command = f"truncate table {step_info.table.name}"
        await execute_query(step_info.table.local_context_dict.connection_string,truncate_command)
    if step_info.load_type == 'merge_and_load':
        table_to_bcp_into, local_context, column_names_list, unique_col_name_list, punique_col_name_list = await initialize_for_merge(step_info, main_table_name)
    step_info.table.name = table_to_bcp_into 
    await panda_file_processor(jobstepid)
    await post_table_load_validator(jobstepid)
    if step_info.load_type == 'merge_and_load':
        await merge_table_and_clean(step_info, main_table_name, table_to_bcp_into, local_context, column_names_list, unique_col_name_list, punique_col_name_list)

async def merge_table_and_clean(step_info, main_table_name, table_to_bcp_into, local_context, column_names_list, unique_col_name_list, punique_col_name_list):
    local_context.target_table = main_table_name
    local_context.source_table = table_to_bcp_into
    local_context.unique_key_columns = unique_col_name_list
    local_context.insert_columns = column_names_list
    local_context.update_columns = [col for col in column_names_list if col not in unique_col_name_list+punique_col_name_list]
    merge_query = render_sql_template('merge_table.sql',local_context)
    print(merge_query)
    await execute_query(step_info.table.local_context_dict.connection_string,merge_query)
    step_info.table.name = main_table_name 
    drop_query = render_sql_template('drop_mtmp_tbl.sql',local_context)
    ic(drop_query)
    await execute_query(step_info.table.local_context_dict.connection_string,drop_query)


async def post_table_load_validator(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    if 'validate' in step_info.file and step_info.file.validate and step_info.file.validate == True:
        #calulcate table row count using table name
        if 'local_context_dict' not in step_info.table:
            set_local_dbvarialbes(step_info.table,jobstepid)
        sql_query = f"select count(1) from {step_info.table.name}"
        table_row_count = get_query_data_pd(step_info.table.local_context_dict.connection_string,sql_query).iloc[0,0]
        if table_row_count != step_info.PROCESSEDROWCNT:
            logger.error(f"Validation failed for PROCESSEDROWCNT vs table row count, Expected {table_row_count=} but got {step_info.PROCESSEDROWCNT=}")
            raise ValueError(f"Validation failed for PROCESSEDROWCNT vs table row count, Expected {table_row_count=} but got {step_info.PROCESSEDROWCNT=}")
        else:
            logger.info(f"Validation passed for PROCESSEDROWCNT vs table row count, Expected {table_row_count=} and got {step_info.PROCESSEDROWCNT=}")
        if table_row_count != step_info.file.EXPECTEDDATALINECOUNT:
            logger.error(f"Validation failed for table row count vs EXPECTEDDATALINECOUNT, Expected {table_row_count=} but got {step_info.file.EXPECTEDDATALINECOUNT=}")
            raise ValueError(f"Validation failed for table row count vs EXPECTEDDATALINECOUNT, Expected {table_row_count=} but got {step_info.file.EXPECTEDDATALINECOUNT=}")
        else:
            logger.info(f"Validation passed for table row count vs EXPECTEDDATALINECOUNT, Expected {table_row_count=} and got {step_info.file.EXPECTEDDATALINECOUNT=}")

async def  preprocess_input_file(file):
    if not os.path.exists(file.FULLFILENAME):
        logger.error(f"Loader process failed, File {file.FULLFILENAME} does not exist")
        raise FileNotFoundError(f"Loader process failed, File {file.FULLFILENAME} does not exist")
    await preprocess_maininputfile_counts(file)

    
    if 'headers_in_order' in file and file.headers_in_order:
        for i in range(len(file.headers_in_order)):
            if 'AUTOPOPULATECOLNAMES' in file.headers_in_order[i]:
                logger.error(f"Loader process failed, AUTOPOPULATECOLNAMES not supported for loader")
        file.headers_in_order = list(map(process_template,file.headers_in_order))
        actual_header_lines = get_n_lines_from_top(file.FULLFILENAME,len(file.headers_in_order))

    ic(file.headers_in_order)
    if 'trailers_in_order' in file and file.trailers_in_order:
        file.trailers_in_order = list(map(process_template,file.trailers_in_order))
        actual_trailer_lines = get_last_n_lines(file.FULLFILENAME,len(file.trailers_in_order))

    ic(file.trailers_in_order)
   

    if 'validate' in file and file.validate and file.validate == True:
        if 'headers_in_order' in file and file.headers_in_order:
            for i,header in enumerate(file.headers_in_order):
                if header:
                    if context_dict.job_cfg.ingnorable_character in header:
                        actual_header_lines[i] = mask_string_for_validation(header, actual_header_lines[i], char_to_mask=context_dict.job_cfg.ingnorable_character)
                    if header != actual_header_lines[i]:
                        logger.error(f"Validation failed for header line {i+1}, Expected {header=} but got {actual_header_lines[i]=}")
                        raise ValueError(f"Validation failed for header line {i+1}, Expected {header=} but got {actual_header_lines[i]=}")
                    else:
                        logger.info(f"Validation passed for header line {i+1}, Expected {header=} and got {actual_header_lines[i]=}")

        if 'trailers_in_order' in file and file.trailers_in_order:
            for i,trailer in enumerate(file.trailers_in_order):
                if trailer:
                    if context_dict.job_cfg.ingnorable_character in trailer:
                        actual_trailer_lines[i] = mask_string_for_validation(trailer, actual_trailer_lines[i], char_to_mask=context_dict.job_cfg.ingnorable_character)
                    if trailer != actual_trailer_lines[i]:
                        logger.error(f"Validation failed for trailer line {i+1}, Expected {trailer=} but got {actual_trailer_lines[i]=}")
                        raise ValueError(f"Validation failed for trailer line {i+1}, Expected {trailer=} but got {actual_trailer_lines[i]=}")
                    else:
                        logger.info(f"Validation passed for trailer line {i+1}, Expected {trailer=} and got {actual_trailer_lines[i]=}")

def initialize_for_outputfile(step_info):
    if 'headers_in_order' in step_info.file and step_info.file.headers_in_order:
        step_info.file.headers_in_order = list(map(process_template,step_info.file.headers_in_order))
        for i in range(len(step_info.file.headers_in_order)):
            if 'AUTOPOPULATECOLNAMES' in step_info.file.headers_in_order[i]:
                local_context = benedict()
                if step_info.type == SQL_BCP_EXTRACTOR or step_info.type == SQL_PANDAS_EXTRACTOR:
                    local_context.base_select_query = read_sql_file(f'sql/{step_info.sqlfile.name}')
                    renderedstring = render_sql_template('cte_for_column_names.sql',local_context)
                    column_names_pd = get_query_data_pd_bydbid(f'{step_info.sqlfile.database}_{step_info.env}',renderedstring)
                else:
                    local_context.base_select_query = f'select * from {step_info.table.name}'
                    renderedstring = render_sql_template('cte_for_column_names.sql',local_context)
                    column_names_pd = get_query_data_pd_bydbid(f'{step_info.table.database}_{step_info.env}',renderedstring)
                #ic(column_names_pd.columns)
                if 'header_delimiter' in step_info.file:
                    sep = step_info.file.header_delimiter
                elif 'delimiter' in step_info.file:
                    sep = step_info.file.delimiter
                else:
                    sep=','
                if 'header_quote_char' in step_info.file:
                    header_quote_char = step_info.file.header_quote_char
                    step_info.file.headers_in_order[i] = step_info.file.headers_in_order[i].replace('AUTOPOPULATECOLNAMES', list_to_string([f'{header_quote_char}{col}{header_quote_char}' for col in column_names_pd.columns.tolist()], sep=sep))
                else:
                    step_info.file.headers_in_order[i] = step_info.file.headers_in_order[i].replace('AUTOPOPULATECOLNAMES', list_to_string(column_names_pd.columns.tolist(),sep=sep))
    if 'ext' not in step_info.file:
            step_info.file.ext = ''

async def process_output_file(file,PROCESSEDROWCNT):
    if 'trailers_in_order' in file and file.trailers_in_order:
        file.trailers_in_order = list(map(process_template,file.trailers_in_order))
        #ic(file.headers_in_order)
    if 'headers_in_order' in file and file.headers_in_order:
        file.headers_in_order = list(map(process_template,file.headers_in_order))
    if not os.path.exists(file.FULLFILENAME):
        logger.error(f"Extractor process failed, File {file.FULLFILENAME} does not exist")
        raise FileNotFoundError(f"Extractor process failed, File {file.FULLFILENAME} does not exist")
    if file.type == 'fixedwidthfile':
        await add_last_empty_line(file.FULLFILENAME)
    add_header_trailer(file.FULLFILENAME, file.headers_in_order, file.trailers_in_order)
    if 'validate' in file and file.validate and file.validate == True:
        file.FILELINECOUNT = get_number_of_lines(file.FULLFILENAME)
        if file.FILELINECOUNT != file.EXPECTEDFILELINECOUNT:
            logger.error(f"Validation failed fpr EXPECTEDFILELINECOUNT vs FILELINECOUNT, Expected {file.EXPECTEDFILELINECOUNT=} but got {file.FILELINECOUNT=}")
            raise ValueError(f"Validation failed for EXPECTEDFILELINECOUNT vs FILELINECOUNT, Expected {file.EXPECTEDFILELINECOUNT=} but got {file.FILELINECOUNT=}")
        else:
            logger.info(f"Validation passed for EXPECTEDFILELINECOUNT vs FILELINECOUNT, Expected {file.EXPECTEDFILELINECOUNT=} and got {file.FILELINECOUNT=}")
        if PROCESSEDROWCNT != file.DATALINECOUNT:
            logger.error(f"Validation failed for PROCESSEDROWCNT vs DATALINECOUNT, Expected {PROCESSEDROWCNT=} but got {file.DATALINECOUNT=}")
            raise ValueError(f"Validation failed, PROCESSEDROWCNT vs DATALINECOUNT, Expected {PROCESSEDROWCNT=} but got {file.DATALINECOUNT=}")
        else:
            logger.info(f"Validation passed PROCESSEDROWCNT vs DATALINECOUNT, Expected {PROCESSEDROWCNT=} and got {file.DATALINECOUNT=}")



async def add_last_empty_line(file_path):
    bash_script = f"""
        if [ -z "$(tail -n 1 {file_path})" ]
        then
            echo "Last line is empty"
        else
            echo "Last line is not empty. Adding an empty line..."
            echo "" >> {file_path}
        fi
    """
    run_subprocess_command(bash_script)



@logger.catch(reraise=True)
async def sql_pandas_extractor(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    process_connecton_string(step_info.sqlfile,jobstepid)
    generate_complete_filename(step_info.file)
    initialize_for_outputfile(step_info)
    await panda_file_processor(jobstepid=jobstepid)
    await process_rawoutputfile_counts(step_info.file)
    await process_output_file(step_info.file,step_info.PROCESSEDROWCNT)
 
def mask_string_for_validation(string_with_mask, string_to_mask, char_to_mask):
    mask_locations = [i for i, char in enumerate(string_with_mask) if char == char_to_mask]
    s2_len = len(string_to_mask)
    string_to_mask = list(string_to_mask)
    for loc in mask_locations:
        if s2_len > loc:            
            string_to_mask[loc] = char_to_mask
    string_to_mask = ''.join(string_to_mask)
    return string_to_mask


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
    check_and_delete_file(temp_file)

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


async def process_rawoutputfile_counts(file_info):
    file_info.DATALINECOUNT = get_number_of_lines(file_info.FULLFILENAME)
    file_info.EXPECTEDFILELINECOUNT = file_info.DATALINECOUNT
    if HEADERS_IN_ORDER in file_info and file_info.headers_in_order:
        file_info.EXPECTEDFILELINECOUNT = file_info.EXPECTEDFILELINECOUNT + len(file_info.headers_in_order)
    if TRAILERS_IN_ORDER in file_info and file_info.trailers_in_order:
        file_info.EXPECTEDFILELINECOUNT = file_info.EXPECTEDFILELINECOUNT + len(file_info.trailers_in_order)
    return file_info

async  def preprocess_maininputfile_counts(file_info):
    file_info.FILELINECOUNT = get_number_of_lines(file_info.FULLFILENAME)
    file_info.EXPECTEDDATALINECOUNT = file_info.FILELINECOUNT
    if HEADERS_IN_ORDER in file_info and file_info.headers_in_order:
        file_info.EXPECTEDDATALINECOUNT = file_info.EXPECTEDDATALINECOUNT - len(file_info.headers_in_order)
    if TRAILERS_IN_ORDER in file_info and file_info.trailers_in_order:
        file_info.EXPECTEDDATALINECOUNT = file_info.EXPECTEDDATALINECOUNT - len(file_info.trailers_in_order)
    return file_info


def render_sql_template(template_name, context_dict):
    # Create the Jinja2 environment
    env = Environment(loader=FileSystemLoader('sql'))
    
    # Load the template
    template = env.get_template(template_name)
    
    # Render the template with the context dictionary
    rendered_sql = template.render(context_dict)
    
    return rendered_sql

#def run_bcp(dbid, env, tablename, filename, bcp_direction, first_row=None, delimiter='\",\"', last_row=None, format_file=None, jobstepid='xxxx'):
async def panda_file_processor(jobstepid=None):
    step_info = context_dict.job_steps_dict[jobstepid]
    file_step_info = step_info.file
    if context_dict.job_steps_dict[jobstepid].type == SQL_PANDAS_EXTRACTOR:
        panda_direction = 'out'
    else:
        panda_direction = 'in'
    filename = file_step_info.FULLFILENAME
    delimiter = None
    quote_char = None
    escape_char = None
    #Unused
    first_row = None
    last_row = None
    widths = None
    alignments = None
    if file_step_info.type == 'delimitedfile':
        if 'delimiter' in file_step_info:
            delimiter = file_step_info.delimiter
        else:
            delimiter = ','
        quoting = None
        if 'quote_char' in file_step_info:
            quote_char = file_step_info.quote_char
            quoting = csv.QUOTE_ALL
        # else:
        #     quote_char = ''
        if 'escape_char' in file_step_info:
            escape_char = file_step_info.escape_char
        else:
            escape_char = '\\'
    elif file_step_info.type == 'fixedwidthfile':
        if 'widhts' not in file_step_info and 'alignments' not in file_step_info and not file_step_info.widths and not file_step_info.alignments:
            raise ValueError("widhts and alignments not provided in file")
        else:
            widths = string_to_list(file_step_info.widths)
            alignments = string_to_list(file_step_info.alignments)
            
    #Unused    
    if 'first_row' in file_step_info:
        first_row = file_step_info.first_row
    if 'last_row' in file_step_info:
        last_row = file_step_info.last_row

  
    if panda_direction == 'in':
        ingestion_file =  filename
        cleaned_file_used=False
        if 'headers_in_order' in file_step_info and file_step_info.headers_in_order:
            remove_header_command = f"sed '1,{len(file_step_info.headers_in_order)}d' {filename} > {filename}.cleaned"
            ic(remove_header_command)
            run_subprocess_command(remove_header_command)
            cleaned_file_used = True
            ingestion_file =  f"{filename}.cleaned"
            header_lines = len(file_step_info.headers_in_order)
        else:
            header_lines = 0
        if 'trailers_in_order' in file_step_info and file_step_info.trailers_in_order:
            tailer_var = file_step_info.FILELINECOUNT - len(file_step_info.trailers_in_order) + 1 - header_lines
            if cleaned_file_used:
                remove_trailer_command = f"sed -i '{tailer_var},$d' {filename}.cleaned"
            else:
                remove_trailer_command = f"sed '{tailer_var},$d' {filename} > {filename}.cleaned"
            print(remove_trailer_command)
            ingestion_file =  f"{filename}.cleaned"
            run_subprocess_command(remove_trailer_command)
    
    if panda_direction == 'out':
        pandacon = get_panda_con(f'{step_info.sqlfile.database}_{step_info.sqlfile.env}')
        set_local_dbvarialbes(step_info.sqlfile,jobstepid)
        pand_select_query = read_sql_file(f'sql/{step_info.sqlfile.name}')
        df = pd.read_sql_query(pand_select_query, pandacon)
        context_dict.job_steps_dict[jobstepid].PROCESSEDROWCNT = len(df)
        if quote_char:
            quoting = csv.QUOTE_ALL
        else:
            quoting = csv.QUOTE_NONE
        if file_step_info.type == 'delimitedfile':
            df.to_csv(filename, index=False, sep=delimiter,quoting=quoting, quotechar=quote_char, escapechar=escape_char, header=False)
        elif file_step_info.type == 'fixedwidthfile':
            if 'dataframe_columns' in step_info.file and step_info.file.dataframe_columns:
                column_list = string_to_list(step_info.file.dataframe_columns)
            else:
                raise ValueError("dataframe_columns not provided in file")
            formatted_rows = df.apply(format_row, axis=1, args=(widths,alignments,column_list))
            with open(filename, 'w') as outfile:
                outfile.write('\n'.join(formatted_rows))
            # Apply the formatter to each row of the DataFrame, then write the result to a file
          
    else:
        pandacon = get_panda_con(f'{step_info.table.database}_{step_info.table.env}')
        chunksize = get_chunksize(step_info,'file')
        set_local_dbvarialbes(step_info.table,jobstepid)
        step_info.PROCESSEDROWCNT = 0
        # Read the CSV file in chunks
        #con = get_con_for_panda(f'{step_info.table.database}_{step_info.table.env}')
        if 'dataframe_columns' in step_info.file and step_info.file.dataframe_columns:
            column_list = string_to_list(step_info.file.dataframe_columns)
        else:
            raise ValueError("dataframe_columns not provided in file")
        #df = pd.read_csv(f"{filename}.cleaned", delimiter=delimiter, quotechar=quote_char,quoting=csv.QUOTE_ALL, escapechar=escape_char , names=column_list)
        engine = create_engine(step_info.table.local_context_dict.connection_string)
        # with engine.begin() as connection:
        #     idenity_insert_on_command = f"SET IDENTITY_INSERT {step_info.table.local_context_dict.basedatabase}.dbo.{step_info.table.name} ON"
        #     connection.execute(text(idenity_insert_on_command))
        #     df.to_sql(step_info.table.name, connection , if_exists='append', schema=f'dbo',index=False)
        # step_info.PROCESSEDROWCNT = len(df)
        
        with engine.begin() as connection:
            #idenity_insert_on_command = f"SET IDENTITY_INSERT {step_info.table.local_context_dict.basedatabase}.dbo.{step_info.table.name} ON"
            idenity_insert_on_command = f"""
                IF EXISTS (
                SELECT 1
                FROM sys.columns
                WHERE object_id = OBJECT_ID('{step_info.table.name}')
                AND is_identity = 1
                )
                BEGIN
                SET IDENTITY_INSERT {step_info.table.name} ON;
                END;
                """
            connection.execute(text(idenity_insert_on_command))
            if quote_char:
                quoting = csv.QUOTE_ALL
            else:
                quoting = csv.QUOTE_NONE


            if file_step_info.type == 'delimitedfile':
                for i, chunk in enumerate(pd.read_csv(f"{ingestion_file}", delimiter=delimiter, quotechar=quote_char,quoting=quoting, escapechar=escape_char , names=column_list, chunksize=chunksize, header=None)):
                    try:
                        # Insert each chunk into the table
                        #con = get_con_for_panda(f'{step_info.table.database}_{step_info.table.env}')
                        #chunk.to_sql(step_info.table.name, con , if_exists='append', schema=f'{step_info.table.local_context_dict.basedatabase}.dbo')  # replace 'my_table' with your actual table name
                        chunk.to_sql(step_info.table.name, connection , if_exists='append', schema=f'dbo',index=False)  # replace 'my_table' with your actual table name
                        step_info.PROCESSEDROWCNT += len(chunk)
                    except Exception as e:
                        print(f"Error processing chunk {i}: {e}")
                        raise e
            elif file_step_info.type == 'fixedwidthfile':
                widths = [int(width) for width in string_to_list(file_step_info.widths)]
                for i, chunk in enumerate(pd.read_fwf(f"{ingestion_file}", widths=widths, names=column_list, chunksize=chunksize, header=None, alignments=alignments)):
                    try:
                        # Insert each chunk into the table
                        #con = get_con_for_panda(f'{step_info.table.database}_{step_info.table.env}')
                        #chunk.to_sql(step_info.table.name, con , if_exists='append', schema=f'{step_info.table.local_context_dict.basedatabase}.dbo')  # replace 'my_table' with your actual table name
                        chunk.to_sql(step_info.table.name, connection , if_exists='append', schema=f'dbo',index=False)  # replace 'my_table' with your actual table name
                        step_info.PROCESSEDROWCNT += len(chunk)
                    except Exception as e:
                        print(f"Error processing chunk {i}: {e}")
                        raise e
                
            idenity_insert_off_command =     f"""
                IF EXISTS (
                SELECT 1
                FROM sys.columns
                WHERE object_id = OBJECT_ID('{step_info.table.name}')
                AND is_identity = 1
                )
                BEGIN
                SET IDENTITY_INSERT {step_info.table.name} OFF;
                END;
            """
            connection.execute(text(idenity_insert_off_command))
        check_and_delete_file(f"{filename}.cleaned")

def get_chunksize(step_info,key):
    chunksize = 12000
    if 'chunksize' in context_dict.job_cfg and context_dict.job_cfg.chunksize:
        chunksize = context_dict.job_cfg.chunksize
    if 'chunksize' in step_info[key] and step_info[key].chunksize:
        chunksize = step_info[key].chunksize
    return chunksize
        

async def simple_file_to_file_transformer(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    check_mandatory_proeperties(step_info,MANDATORT_SIMPLE_FILE_TO_FILE_PROPERTIES)
    generate_complete_filename(step_info.filei)
    generate_complete_filename(step_info.fileo)
    await preprocess_input_file(step_info.filei)
  
    file_step_info_i, delimiter_i, quote_char_i, escape_char_i, widths_i, alignments_i, quoting_i, column_list_i = initilize_common_file_properties(step_info,'filei')   
    file_step_info_o, delimiter_o, quote_char_o, escape_char_o, widths_o, alignments_o, quoting_o, column_list_o = initilize_common_file_properties(step_info,'fileo')

    ingestion_file = cleanheader_and_trailer(file_step_info_i)
    ic(ingestion_file)

    if 'chunksize' in context_dict.job_cfg and context_dict.job_cfg.chunksize:
        chunksize = context_dict.job_cfg.chunksize

    
    output_file =  file_step_info_o.FULLFILENAME
    step_info.PROCESSEDROWCNT = 0
    check_and_delete_file(output_file)
    if file_step_info_i.type == 'delimitedfile':
        widths = [int(width) for width in string_to_list(file_step_info_o.widths)]
        df = pd.read_csv(f"{ingestion_file}", delimiter=delimiter_i, quotechar=quote_char_i,quoting=quoting_i, escapechar=escape_char_i , names=column_list_i, header=None)
        formatted_rows = df.apply(format_row, axis=1, args=(widths,alignments_o,column_list_o))
        with open(output_file, 'a') as outfile:
            outfile.write('\n'.join(formatted_rows))
        step_info.PROCESSEDROWCNT = len(df)
        # for i, chunk in enumerate(pd.read_csv(f"{ingestion_file}", delimiter=delimiter_i, quotechar=quote_char_i,quoting=quoting, escapechar=escape_char_i , names=column_list_i, chunksize=chunksize, header=None)):
        #     try:
        #         formatted_rows = chunk.apply(format_row, axis=1, args=(widths,alignments_o,column_list_o))
        #         with open(output_file, 'a') as outfile:
        #             outfile.write('\n'.join(formatted_rows))
        #         step_info.PROCESSEDROWCNT += len(chunk)
        #     except Exception as e:
        #         print(f"Error processing chunk {i}: {e}")
        #         raise e
    elif file_step_info_i.type == 'fixedwidthfile':
        widths = [int(width) for width in string_to_list(file_step_info_i.widths)]
        for i, chunk in enumerate(pd.read_fwf(f"{ingestion_file}", widths=widths, names=column_list_i, chunksize=chunksize, header=None, alignments=alignments_i)):
            try:
              chunk.to_csv(output_file, index=False, sep=delimiter_o,quoting=quoting_o, quotechar=quote_char_o, escapechar=escape_char_o, header=False, mode='a')
              step_info.PROCESSEDROWCNT += len(chunk)
            except Exception as e:
                print(f"Error processing chunk {i}: {e}")
                raise e
    ic(step_info.PROCESSEDROWCNT)
    ic(step_info.filei.FILELINECOUNT)
    await process_rawoutputfile_counts(step_info.fileo)
    await process_output_file(step_info.fileo,step_info.PROCESSEDROWCNT)
    if 'cleaned' in ingestion_file:
        check_and_delete_file(ingestion_file)
    

def cleanheader_and_trailer(file_step_info):
    ingestion_file =  file_step_info.FULLFILENAME
    check_and_delete_file(f"{file_step_info.FULLFILENAME}.cleaned")
    cleaned_file_used=False
    if 'headers_in_order' in file_step_info and file_step_info.headers_in_order:
        remove_header_command = f"sed '1,{len(file_step_info.headers_in_order)}d' {file_step_info.FULLFILENAME} > {file_step_info.FULLFILENAME}.cleaned"
        ic(remove_header_command)
        run_subprocess_command(remove_header_command)
        cleaned_file_used = True
        ingestion_file =  f"{file_step_info.FULLFILENAME}.cleaned"
        header_lines = len(file_step_info.headers_in_order)
    else:
        header_lines = 0
    if 'trailers_in_order' in file_step_info and file_step_info.trailers_in_order:
        tailer_var = file_step_info.FILELINECOUNT - len(file_step_info.trailers_in_order) + 1 - header_lines
        if cleaned_file_used:
            remove_trailer_command = f"sed -i '{tailer_var},$d' {file_step_info.FULLFILENAME}.cleaned"
        else:
            remove_trailer_command = f"sed '{tailer_var},$d' {file_step_info.FULLFILENAME} > {file_step_info.FULLFILENAME}.cleaned"
        print(remove_trailer_command)
        ingestion_file =  f"{file_step_info.FULLFILENAME}.cleaned"
        run_subprocess_command(remove_trailer_command)
    return ingestion_file



def initilize_for_duckdb(delimiter_x, quote_char_x, escape_char_x):
    if delimiter_x:
        sep_x = f", sep = '{delimiter_x}'"
    else:
        sep_x = ""
    if quote_char_x:
        quote_x = f", quote = '{quote_char_x}'"
    else:
        quote_x = ""
    if escape_char_x:
        escape_x = f", escape = '{escape_char_x}'"
    else:
        escape_x = ""
    return sep_x,quote_x,escape_x

def initilize_common_file_properties(step_info,key):
    file_step_info_x = step_info[key]
    delimiter_x = None
    quote_char_x = None
    escape_char_x = None
    widths_x = None
    quoting_x = None
    alignments_x = None
    if file_step_info_x.type == 'delimitedfile':
        if 'delimiter' in file_step_info_x:
            delimiter_x = file_step_info_x.delimiter
        else:
            delimiter_x = ','
        quoting_x = csv.QUOTE_NONE
        if 'quote_char' in file_step_info_x:
            quote_char_x = file_step_info_x.quote_char
            quoting_x = csv.QUOTE_ALL
        if 'escape_char' in file_step_info_x:
            escape_char_x = file_step_info_x.escape_char
        else:
            escape_char_x = '\\'
    elif file_step_info_x.type == 'fixedwidthfile':
        if 'widhts' not in file_step_info_x and 'alignments' not in file_step_info_x and not file_step_info_x.widths and not file_step_info_x.alignments:
            raise ValueError("widhts and alignments not provided in file")
        else:
            widths_x = string_to_list(file_step_info_x.widths)
            alignments_x = string_to_list(file_step_info_x.alignments)
    if 'dataframe_columns' in step_info[key] and step_info[key].dataframe_columns:
        column_list_x = string_to_list(step_info[key].dataframe_columns)
    else:
        raise ValueError(f"dataframe_columns not provided in {key}")
    return file_step_info_x,delimiter_x,quote_char_x,escape_char_x,widths_x,alignments_x,quoting_x,column_list_x




def panda_file_writer(step_info, df, file_step_info_o, delimiter_o, quote_char_o, escape_char_o, widths_o, alignments_o, quoting_o, column_list_o):
    check_and_delete_file(file_step_info_o.FULLFILENAME)
    if file_step_info_o.type == 'delimitedfile':
        df.to_csv(file_step_info_o.FULLFILENAME , index=False, sep=delimiter_o,quoting=quoting_o, quotechar=quote_char_o, escapechar=escape_char_o, header=False)
    elif file_step_info_o.type == 'fixedwidthfile':
        formatted_rows = df.astype(str).apply(format_row, axis=1, args=(widths_o,alignments_o,column_list_o))
        with open(file_step_info_o.FULLFILENAME, 'w') as outfile:
            outfile.write('\n'.join(formatted_rows))
    step_info.PROCESSEDROWCNT = len(df)