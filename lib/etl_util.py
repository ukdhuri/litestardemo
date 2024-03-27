

import platform
import re
import subprocess

from pendulum import date
import pendulum
from benedict import benedict
from .etl_context import context_dict
from loguru import logger

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


@logger.catch(reraise=True)
def repl(match):
    var_name = match.group(1)
    var_type = match.group(2)
    format_string = match.group(3)

    var_value = context_dict.get(var_name)
    
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

    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    output = result.stdout.strip()

@logger.catch(reraise=True)
def create_pair_list(lst):
    pair_list = []
    for i in range(0, len(lst), 2):
        pair_list.append((int(lst[i]), int(lst[i+1])))
    return pair_list      


#index starts at 1
@logger.catch(reraise=True)
def get_nth_line_from_top(file_path, n):
    if platform.system() == 'Windows':
        command = f"powershell -Command \"Get-Content -Path '{file_path}' -TotalCount {n} | Select-Object -First {n}\""
    else:
        command = f"head -n {n} {file_path} | tail -1"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    output = result.stdout.strip()
    return output

# index starts at 1
@logger.catch(reraise=True)
def get_nth_line_from_bottom(file_path, n):
    if platform.system() == 'Windows':
        command = f"powershell -Command \"Get-Content -Path '{file_path}' -Tail {n}\""
    else:
        command = f"tail -n {n} {file_path} | head -1"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    output = result.stdout.strip()
    return output 

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