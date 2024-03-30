

import pathlib
import platform
import re
import subprocess

from pendulum import date
import pendulum
from benedict import benedict
from .etl_context import context_dict
from loguru import logger
import os

def decrpyt_password(cybeark_string):
    p = subprocess.Popen(cybeark_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, _ = p.communicate()
    output_str = output.decode('utf-8')
    if "ERROR" in output_str.upper():
        print("Error")
        exit(1)
    return output_str.strip()


async def read_batch_date_file(batch_date_path,format) -> date:
    with open(batch_date_path, "r") as file:
        batch_date_str = file.read().strip()
        batch_date = pendulum.from_format(batch_date_str, "YYYYMMDD").date()
        return batch_date

async def get_batch_date_str(batch_date_path,format) -> date:
    return await read_batch_date_file(batch_date_path,format).format(format)

async def get_current_date_time_str(format):
    return pendulum.now().format(format)


def check_and_delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"File {file_path} deleted successfully.")
    else:
        logger.info(f"File {file_path} does not exist.")


@logger.catch(reraise=True)
def repl(match):
    var_name = match.group(1)
    var_type = match.group(2)
    format_string = match.group(3)

    if var_name == 'CDATE' or var_name == 'CDATETIME' or var_name == 'CTIME':
        var_value = pendulum.now()
    else:
        if not context_dict.job_cfg.child_access_symbol in var_name:
            var_value = context_dict[var_name]
        else:
            var_value = context_dict.job_steps_dict[var_name.replace(context_dict.job_cfg.child_access_symbol,'.')]
    # Format the variable based on its type
    if var_type == 'date':
        formatted_var = var_value.format(format_string)
    elif var_type == 'float':
        formatted_var = f"{var_value:{format_string}}"
    elif var_type == 'str':
        formatted_var = f"{var_value:{format_string}}"
    elif var_type == 'int':
        formatted_var = f"{int(var_value):{format_string}}"
    else:
        raise ValueError(f"Unknown variable type: {var_type}")

    return formatted_var


@logger.catch(reraise=True)
def process_template(template: str) -> str:
    return re.sub(context_dict.pattern, repl, template)

@logger.catch(reraise=True)
def read_yaml_file(yaml_file_path):
    config = benedict.from_yaml(yaml_file_path)
    return config

@logger.catch(reraise=True)
def get_number_of_lines(file_path):
    if platform.system() == 'Linux':
        command = f"wc -l {file_path}"
    elif platform.system() == 'Windows':
        command = f"powershell -Command \"(Get-Content -Path '{file_path}').Count\""
    else:
        raise OSError("Unsupported operating system")
    return int(run_subprocess_command(command))


@logger.catch(reraise=True)
def create_pair_list(lst):
    pair_list = []
    for i in range(0, len(lst), 2):
        pair_list.append((int(lst[i]), int(lst[i+1])))
    return pair_list      


#index starts at 1
@logger.catch(reraise=True)
def get_n_lines_from_top(file_path, n):
    if platform.system() == 'Windows':
        command = f"powershell -Command \"Get-Content -Path '{file_path}' -TotalCount {n}\""
    else:
        command = f"head -n {n} {file_path}"
    output = run_subprocess_command(command)
    lines = output.splitlines()
    return lines

@logger.catch(reraise=True)
def get_nth_line_from_top(file_path, n):
    if platform.system() == 'Windows':
        command = f"powershell -Command \"Get-Content -Path '{file_path}' -TotalCount {n} | Select-Object -First {n}\""
    else:
        command = f"head -n {n} {file_path} | tail -1"
    return run_subprocess_command(command)

# index starts at 1
@logger.catch(reraise=True)
def get_nth_line_from_bottom(file_path, n):
    if platform.system() == 'Windows':
        command = f"powershell -Command \"Get-Content -Path '{file_path}' -Tail {n}\""
    else:
        command = f"tail -n {n} {file_path} | head -1"
    return run_subprocess_command(command)


def get_last_n_lines(file_path, n):
    if platform.system() == 'Windows':
        command = f"powershell -Command \"Get-Content -Path '{file_path}' -Tail {n}\""
    else:
        command = f"tail -n {n} {file_path}"
    output = run_subprocess_command(command)
    lines = output.splitlines()
    return lines

@logger.catch(reraise=True)
def remove_text_between_positions(text, positions):
    offset = 0
    for start, end in positions:
        end += 1
        start -= offset
        end -= offset
        text = text[:start] + text[end:]
        offset += end - start
    return text 

@logger.catch(reraise=True)
def list_to_string(lst : list[str],sep=',') -> str:
    if not lst or len(lst) == 0:
        return ''
    return sep.join(str(item) for item in lst)

@logger.catch(reraise=True)
def string_to_list(string: str,sep=',') -> list[str]:
    if not string or len(string) == 0:
        return []
    if sep not in string:
        return [string]
    return string.split(sep)


@logger.catch(reraise=True)
def clean_meta_records(metasyntax,text):
    if metasyntax != 'ALL':
        for pos in string_to_list(metasyntax):
            if not pos.isdigit():
                print('metasyntax is not valid')
                exit(1)
        headr_positions = string_to_list(metasyntax)
        if len(headr_positions) % 2 != 0:
            print('metasyntax is not valid, odd vlaues in synatax')
        
        header_pair_list = create_pair_list(headr_positions)
        clean_header = remove_text_between_positions(text,header_pair_list)
        return clean_header
    else:
        return text

@logger.catch(reraise=True)
def get_number_of_lines(file_path):
    """
    Returns the number of lines in a given file.

    Args:
        file_path (str): The path to the file.

    Returns:
        int: The number of lines in the file.

    Raises:
        OSError: If the operating system is not supported.
    """
    if platform.system() == 'Linux':
        awkcom="awk '{print $1}'"
        command = f"wc -l {file_path} | {awkcom} "
    elif platform.system() == 'Windows':
        command = f"powershell -Command \"(Get-Content -Path '{file_path}').Count\""
    else:
        raise OSError("Unsupported operating system")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    output = result.stdout.strip()
    return int(output)

def process_job_steps(list_of_dicts):
    """
    Process a list of dictionaries and returns two dictionaries.

    Args:
        list_of_dicts (list): A list of dictionaries.

    Returns:
        tuple: A tuple containing two dictionaries. The first dictionary maps the keys of the input dictionaries
        to their corresponding index in the list. The second dictionary combines all the dictionaries in the list,
        with duplicate keys overwritten by the last occurrence.

    Example:
        >>> list_of_dicts = [{'a': 1}, {'b': 2}, {'c': 3}]
        >>> process_job_steps(list_of_dicts)
        ({'a': 0, 'b': 1, 'c': 2}, {'a': 1, 'b': 2, 'c': 3})
    """
    dict1 = benedict()
    for i, d in enumerate(list_of_dicts):
        if len(d) != 1:
            raise ValueError(f"Dictionary at index {i} has more than one key")
        dict1[i] = list(d.keys())[0]
    dict2 = {k: v for d in list_of_dicts for k, v in d.items()}
    return dict1, dict2


def check_key_value(dictionary, key):
    if key not in dictionary or dictionary[key] is None or str(dictionary[key]) == '':
        return False
    return True

@logger.catch(reraise=True)
def run_subprocess_command(command,check_error=True,check_status=True,show_command="",additional_log_files=[]):
    if not show_command:
        show_command = command
    logger.info(f"Executing subprocess command {show_command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    output_str = result.stdout.strip()
    error_message = ""
    if check_status:
        if result.returncode != 0:
            logger.error(f"Subprocess command {show_command} failed with return code {result.returncode} and error message {result.stderr} with output {output_str}")
            if result.stderr:
                error_message = f"Std Error ⚠️ {result.stderr} ⚠️"
            for log_file in additional_log_files:
                if os.path.exists(log_file):
                    logger.info(f"Additional log file {log_file} content: {read_file(log_file)}")
            raise Exception("Subprocess execution failed")
    if check_error:
        if "ERROR" in output_str.upper() or 'FAILED' in output_str.upper() or 'EXCEPTION' in output_str.upper() or 'TRACEBACK' in output_str.upper() or 'STACKTRACE' in output_str.upper():
            if result.stderr:
                error_message = f"Std Error ⚠️ {result.stderr} ⚠️"
            logger.error(f"Subprocess command {show_command} completed with return code {result.returncode} with output {output_str} ")
            for log_file in additional_log_files:
                if os.path.exists(log_file):
                    log_content = read_file(log_file)
                    logger.info(f"Additional log file {log_file} content: {read_file(log_file)}")
            raise Exception("Subprocess execution failed") 
    if result.returncode == 0:
        logger.info(f"Subprocess command {show_command} executed successfully with output {output_str} -- {result.stderr}")
    status_code = result.returncode
    if result.stderr:
        error_message = f"Std Error ⚠️ {result.stderr} ⚠️"
        logger.info(f"Subprocess command {show_command} executed with status code {status_code} and output \n{output_str} {error_message} ")
    return output_str

def check_mandatory_proeperties(dictionary, items):
    for item in items:
        if item not in dictionary:
            logger.error(f"Property '{item}' not found in the configuration")
            raise Exception(f"Property '{item}' not found in the configuration")
        

def read_file(file_path):
    """
    Read the content of a file and return it as a string.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The content of the file as a string.
    """
    with open(file_path, "r") as file:
        content = file.read()
    return content

def read_sql_file(file_path):
    """
    Read the content of a SQL file and return it as a string.

    Args:
        file_path (str): The path to the SQL file.

    Returns:
        str: The content of the SQL file as a string.
    """
    return read_file(file_path = str(pathlib.Path(file_path)))