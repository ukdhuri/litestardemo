import csv
import glob
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
from lib.etl_service import cleanheader_and_trailer, generate_complete_filename, preprocess_input_file, process_output_file, process_rawoutputfile_counts
from .etl_util import check_and_delete_file, check_mandatory_proeperties, decrpyt_password, format_row, get_last_n_lines, get_n_lines_from_top, get_number_of_lines, list_to_string, process_template, read_yaml_file, run_subprocess_command, read_sql_file, string_to_list
from .etl_context import context_dict
import subprocess
from pathlib import Path
import os
from jinja2 import Environment, FileSystemLoader
import duckdb


async def trasform_files_sample(jobstepid):
    step_info = context_dict.job_steps_dict[jobstepid]
    #check_mandatory_proeperties(step_info,MANDATORT_SIMPLE_FILE_TO_FILE_PROPERTIES)
    generate_complete_filename(step_info.filei1)
    generate_complete_filename(step_info.filei2)
    generate_complete_filename(step_info.fileo)
    await preprocess_input_file(step_info.filei1)
    await preprocess_input_file(step_info.filei2)

    file_step_info_i1 = step_info.filei1
    delimiter_i1 = None
    quote_char_i1 = None
    escape_char_i1 = None
    widths_i1 = None
    alignments_i1 = None
    if file_step_info_i1.type == 'delimitedfile':
        if 'delimiter' in file_step_info_i1:
            delimiter_i1 = file_step_info_i1.delimiter
        else:
            delimiter_i1 = ','
        quoting_i1 = csv.QUOTE_NONE
        if 'quote_char' in file_step_info_i1:
            quote_char_i1 = file_step_info_i1.quote_char
            quoting_i1 = csv.QUOTE_ALL
        # else:
        #     quote_char = ''
        if 'escape_char' in file_step_info_i1:
            escape_char_i1 = file_step_info_i1.escape_char
        else:
            escape_char_i1 = '\\'
    elif file_step_info_i1.type == 'fixedwidthfile':
        if 'widhts' not in file_step_info_i1 and 'alignments' not in file_step_info_i1 and not file_step_info_i1.widths and not file_step_info_i1.alignments:
            raise ValueError("widhts and alignments not provided in file")
        else:
            widths_i1 = string_to_list(file_step_info_i1.widths)
            alignments_i1 = string_to_list(file_step_info_i1.alignments)

    if 'dataframe_columns' in step_info.filei1 and step_info.filei1.dataframe_columns:
        column_list_i1 = string_to_list(step_info.filei1.dataframe_columns)
    else:
        raise ValueError("dataframe_columns not provided in filei1")
        
    file_step_info_i2 = step_info.filei2
    delimiter_i2 = None
    quote_char_i2 = None
    escape_char_i2 = None
    widths_i2 = None
    alignments_i2 = None
    if file_step_info_i2.type == 'delimitedfile':
        if 'delimiter' in file_step_info_i2:
            delimiter_i2 = file_step_info_i2.delimiter
        else:
            delimiter_i2 = ','
        quoting_i2 = csv.QUOTE_NONE
        if 'quote_char' in file_step_info_i2:
            quote_char_i2 = file_step_info_i2.quote_char
            quoting_i2 = csv.QUOTE_ALL
        # else:
        #     quote_char = ''
        if 'escape_char' in file_step_info_i2:
            escape_char_i2 = file_step_info_i2.escape_char
        else:
            escape_char_i2 = '\\'
    elif file_step_info_i2.type == 'fixedwidthfile':
        if 'widhts' not in file_step_info_i2 and 'alignments' not in file_step_info_i2 and not file_step_info_i2.widths and not file_step_info_i2.alignments:
            raise ValueError("widhts and alignments not provided in file")
        else:
            widths_i2 = string_to_list(file_step_info_i2.widths)
            alignments_i2 = string_to_list(file_step_info_i2.alignments)
    if 'dataframe_columns' in step_info.filei2 and step_info.filei2.dataframe_columns:
        column_list_i2 = string_to_list(step_info.filei2.dataframe_columns)
    else:
        raise ValueError("dataframe_columns not provided in filei2")

    delimiter_o = None
    quote_char_o = None
    escape_char_o = None
    widths_o = None
    alignments_o = None
    file_step_info_o = step_info.fileo
    if file_step_info_o.type == 'delimitedfile':
        if 'delimiter' in file_step_info_o:
            delimiter_o= file_step_info_o.delimiter
        else:
            delimiter_o= ','
        quoting_o = csv.QUOTE_NONE
        if 'quote_char' in file_step_info_o:
            quote_char_o= file_step_info_o.quote_char
            quoting_o = csv.QUOTE_ALL
        # else:
        #     quote_char = ''
        if 'escape_char' in file_step_info_o:
            escape_char_o= file_step_info_o.escape_char
        else:
            escape_char_o= '\\'
    elif file_step_info_o.type == 'fixedwidthfile':
        if 'widhts' not in file_step_info_o and 'alignments' not in file_step_info_o and not file_step_info_o.widths and not file_step_info_o.alignments:
            raise ValueError("widhts and alignments not provided in file")
        else:
            widths_o= string_to_list(file_step_info_o.widths)
            alignments_o= string_to_list(file_step_info_o.alignments)   
    if 'dataframe_columns' in step_info.fileo and step_info.fileo.dataframe_columns:
        column_list_o = string_to_list(step_info.fileo.dataframe_columns)
    else:
        raise ValueError("dataframe_columns not provided in fileo")


    ingestion_file1 = cleanheader_and_trailer(file_step_info_i1)
    ingestion_file2 = cleanheader_and_trailer(file_step_info_i2)

    if 'chunksize' in context_dict.job_cfg and context_dict.job_cfg.chunksize:
        chunksize = context_dict.job_cfg.chunksize
    
    output_file =  file_step_info_o.FULLFILENAME
    step_info.PROCESSEDROWCNT = 0
    check_and_delete_file(output_file)

    duckcon = duckdb.connect()
    if delimiter_i1:
        sep_i1 = f", sep = '{delimiter_i1}'"
    else:
        sep_i1 = ""
    if quote_char_i1:
        quote_i1 = f", quote = '{quote_char_i1}'"
    else:
        quote_i1 = ""
    if escape_char_i1:
        escape_i1 = f", escape = '{escape_char_i1}'"
    else:
        escape_i1 = ""
    #duckq = f"SELECT * FROM read_csv('{ingestion_file1}'  {sep_i1} {quote_i1} {escape_i1}  , header = false);"
    duckqi1 = f"SELECT * FROM read_csv('{ingestion_file1}' {sep_i1} {quote_i1} , header = false, column_names = [{step_info.filei1.dataframe_columns}]);"
    print(duckqi1)
    resulti1 = duckcon.execute(duckqi1)
    print(resulti1.fetchdf())


    if delimiter_i2:
        sep_i2 = f", sep = '{delimiter_i2}'"
    else:
        sep_i2 = ""
    if quote_char_i2:
        quote_i2 = f", quote = '{quote_char_i2}'"
    else:
        quote_i2 = ""
    if escape_char_i2:
        escape_i2 = f", escape = '{escape_char_i2}'"
    else:
        escape_i2 = ""
    duckqi2 = f"SELECT * FROM read_csv('{ingestion_file2}' {sep_i2} {quote_i2} , header = false, column_names = [{step_info.filei2.dataframe_columns}]);"
    print(duckqi2)
    resulti2 = duckcon.execute(duckqi2)
    print(resulti2.fetchdf())


    final_query = f"""SELECT ordercsv.id as order_id, usercsv.name as user_name, usercsv.email email, ordercsv.batch_id, ordercsv.start_time FROM 
    read_csv('{ingestion_file1}' {sep_i1} {quote_i1} , header = false, column_names = [{step_info.filei1.dataframe_columns}]) as ordercsv JOIN 
    read_csv('{ingestion_file2}' {sep_i2} {quote_i2} , header = false, column_names = [{step_info.filei2.dataframe_columns}]) as usercsv ON ordercsv.user_id = usercsv.id;
    """
    print(final_query)
    final_result_df = duckcon.execute(final_query).fetchdf()
    print(final_result_df)


    if file_step_info_o.type == 'delimitedfile':
        final_result_df.to_csv(file_step_info_o.FULLFILENAME , index=False, sep=delimiter_o,quoting=quoting_o, quotechar=quote_char_o, escapechar=escape_char_o, header=False)
    elif file_step_info_o.type == 'fixedwidthfile':
        formatted_rows = final_result_df.astype(str).apply(format_row, axis=1, args=(widths_o,alignments_o,column_list_o))
        with open(file_step_info_o.FULLFILENAME, 'w') as outfile:
            outfile.write('\n'.join(formatted_rows))
    step_info.PROCESSEDROWCNT = len(final_result_df)
    await process_rawoutputfile_counts(step_info.fileo)
    await process_output_file(step_info.fileo,step_info.PROCESSEDROWCNT)
    if 'cleaned' in ingestion_file1:
        check_and_delete_file(ingestion_file1)
    if 'cleaned' in ingestion_file2:
        check_and_delete_file(ingestion_file2)

    #if file_step_info_i1.type == 'delimitedfile':
    #     widths = [int(width) for width in string_to_list(file_step_info_o.widths)]
    #     df = pd.read_csv(f"{ingestion_file}", delimiter=delimiter_i1, quotechar=quote_char_i1,quoting=quoting_i, escapechar=escape_char_i1 , names=column_list_i, header=None)
    #     formatted_rows = df.apply(format_row, axis=1, args=(widths,alignments_o,column_list_o))
    #     with open(output_file, 'a') as outfile:
    #         outfile.write('\n'.join(formatted_rows))
    #     step_info.PROCESSEDROWCNT = len(df)
    # elif file_step_info_i1.type == 'fixedwidthfile':
    #     widths = [int(width) for width in string_to_list(file_step_info_i1.widths)]
    #     for i, chunk in enumerate(pd.read_fwf(f"{ingestion_file}", widths=widths, names=column_list_i, chunksize=chunksize, header=None, alignments=alignments_i1)):
    #         try:
    #           chunk.to_csv(output_file, index=False, sep=delimiter_o,quoting=quoting_o, quotechar=quote_char_o, escapechar=escape_char_o, header=False, mode='a')
    #           step_info.PROCESSEDROWCNT += len(chunk)
    #         except Exception as e:
    #             print(f"Error processing chunk {i}: {e}")
    #             raise e
    # ic(step_info.PROCESSEDROWCNT)
    # ic(step_info.filei.FILELINECOUNT)
    # await process_rawoutputfile_counts(step_info.fileo)
    # await process_output_file(step_info.fileo,step_info.PROCESSEDROWCNT)
    # if 'cleaned' in ingestion_file:
    #     check_and_delete_file(ingestion_file)
