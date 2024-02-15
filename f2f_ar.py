import random
import shutil
import pandas as pd
from icecream import ic
from faker import Faker
from datetime import datetime, timedelta,time
import pendulum
from pathlib import Path
import pyarrow as pa
import os
import pyarrow.csv as csv
import io 
import subprocess
from tabulate import tabulate
import time as tx
def read_config(file_path):
    # Read the Excel file
    excel_data = pd.read_excel(file_path, sheet_name=None)
    # Extract the necessary information from the sheets
    meta1 = excel_data['meta1']
    meta2 = excel_data['meta2']
    colmapping = excel_data['colmapping']
    # Get the FILE_PATH from meta1 and meta2
    file_path1 = meta1.loc[meta1['MKey'] == 'FILE_PATH', 'MValue'].values[0]
    file_path2 = meta2.loc[meta2['MKey'] == 'FILE_PATH', 'MValue'].values[0]
    filename1 = 'abc.txt'
    filename2 = 'xyz.txt'
    ic(meta1)
    ic(meta2)
    ic(colmapping)
    # Create the complete file names
    filename1 = Path(file_path1,filename1) 
    filename2 = Path(file_path2,filename2) 
    return colmapping,meta1,meta2,filename1,filename2

class StreamWrapper():
    
    def __init__(self, obj=None):
        self.delegation = obj
        self.header = None

    def __getattr__(self, *args, **kwargs):
        # any other call is delegated to the stream/object
        return(self.delegation.__getattribute__(*args, **kwargs))

    def read(self, size=None, *args, **kwargs):
        bytedata = self.delegation.read(size, *args, **kwargs)          
        # .. the rest of the logic pre-processes the byte data, 
        # identifies the header and number of columns (which are retained persistently),
        # and then strips out extra columns when encountered
        return(bytedata)


def main():
    file_path = '/home/deck/Documents/mappings/test1_F2F.xlsx'
    colmapping, meta1, meta2, filename1, filename2 = read_config(file_path)
    list_of_columns = list(colmapping['filecolname'])
    delmt=meta1.loc[meta1['MKey'] == 'DELIMETER', 'MValue'].values[0]
    create_csv_file(colmapping,filename1,delmt)
    exit()



    lenghtlist = list(colmapping['length'])
    ic(list_of_columns)

    # read_options = csv.ReadOptions(skip_rows=1)
    # script_path = '/home/deck/devlopment/demo/cleanheadandtail.sh'
    # param1 = f'{filename1}'
    # param2 = '0'
    # param3 = '1' 

    # read_options = csv.ReadOptions(column_names = list_of_columns,skip_rows=1)
    # script_path = '/home/deck/devlopment/demo/cleanheadandtail.sh'
    # param1 = f'{filename1}'
    # param2 = '1'
    # param3 = '1'


    read_options = csv.ReadOptions(column_names = list_of_columns,skip_rows=0)
    script_path = '/home/deck/devlopment/demo/cleanheadandtail.sh'
    #filename1= f'{filename1}.fl'
    param1 = f'{filename1}'
    param2 = '2'
    param3 = '1'


    #Execute the shell script with the parameters
    subprocess.run([script_path, param1, param2, param3])
    start_time = tx.perf_counter()


    # Read the fixed length file
    
    #dx=csv.read_csv(f'{filename1}.gz',read_options=read_options,parse_options=csv.ParseOptions(delimiter=delmt,escape_char='\\')).to_pandas()
    dx = pd.read_csv(f'{filename1}.gz',engine='pyarrow', dtype_backend='pyarrow', delimiter=delmt, names=list_of_columns, skiprows=0, header=None, encoding='utf-8' ,escapechar='\\', compression='gzip')
    #dx = pd.read_fwf(f'{filename1}.gz', widths=lenghtlist, names=list_of_columns,engine='pyarrow', dtype_backend='pyarrow', compression='gzip')


    end_time = tx.perf_counter()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
    type(dx)

    ic(dx[:5])
    ic(dx[-5:])


    #print(dx.engine)
    print(dx.dtypes)

    #dx.to_string('fixed_length.txt', index=False)
    # content = tabulate(dx.values.tolist(), list(dx.columns), tablefmt="plain")
    # open('fixed_length.txt', "w").write(content)






def create_csv_file(colmapping, filename1,sep):
    df = create_large_df(50, list(colmapping['filecolname']), list(colmapping['type']), list(colmapping['length']))
    header1 = 'filename\n'
    header2 = ','.join(df.columns)
    # Remove filename1 if exists
    if os.path.exists(filename1):
        os.remove(filename1)
        
    start_time = tx.perf_counter()
    ic(sep)

    df.to_csv(filename1, index=False, sep=sep, header=list(df.columns), quotechar='"', quoting=1, escapechar='\\')

    end_time = tx.perf_counter()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

    # Add trailer with record count
    record_count = len(df)
    trailer = f'RECORDCOUNT={record_count}\n'

    with open(filename1, 'a') as file:
        file.write(trailer)
    # with open(filename1, 'r+') as f:
    #     content = f.read()
    #     f.seek(0, 0)
    #     f.write('new first line\n' + content)
    temp_filename = 'temp.txt'   
    if os.path.exists(temp_filename):
        os.remove(temp_filename)
    with open(filename1, 'r') as original_file, open(temp_filename, 'w') as temp_file:
        temp_file.write(header1)
        shutil.copyfileobj(original_file,temp_file)
    ic(filename1)
    os.remove(filename1)
    ic(temp_filename)
    shutil.move(temp_filename, filename1)
    shutil.copy(filename1, f'{filename1}.bak2')
    ic(filename1)

    script_path = '/home/deck/devlopment/demo/fixedlenghtconv.sh'
    subprocess.run([script_path, filename1])



def create_large_df(num_rows, columns=['A', 'B'], column_types=['int', 'int'],column_lengnths=[10, 10]):
    fake = Faker()
    # Create a dataframe with random values
    df = pd.DataFrame()
    start = pendulum.datetime(1970, 1, 1)
    end = pendulum.now()
    delta = end - start
    i = 0
    for col, col_type, col_length in zip(columns, column_types, column_lengnths):
        ic(i)
        if col_type == 'int':
            if i == 0:
                df[col] = [str(i).zfill(col_length) for i in range(1, num_rows+1)]
            else:
                df[col] = [str(random.randint(1, 1000000)).zfill(col_length)[:col_length] for _ in range(num_rows)]
        elif col_type == 'float':
            df[col] = [str(fake.pydecimal(left_digits=15, right_digits=2, positive=True)).zfill(col_length)[-col_length:] for _ in range(num_rows)]
        elif col_type == 'str':
            df[col] = [fake.name().ljust(col_length)[:col_length] for _ in range(num_rows)]
        elif col_type == 'date':
            df[col] = [start.add(seconds=random.randint(0, delta.in_seconds())).to_date_string() for _ in range(num_rows)]
        elif col_type == 'datetime':
            df[col] = [start.add(seconds=random.randint(0, delta.in_seconds())).to_datetime_string() for _ in range(num_rows)]
        i += 1
    return df




if __name__ == "__main__":
    main()