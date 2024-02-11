import json
import queue
import subprocess
from typing import Optional
from benedict import benedict
from faker import Faker
from loguru import logger
from omegaconf import DictConfig
import pandas as pd
from sqlalchemy import Engine
from sqlmodel import SQLModel, create_engine, select
from datetime import datetime,date,time,timedelta
from litestar.datastructures import State
from hydra import compose, initialize_config_dir
from models import local
import os
# from sqlalchemy.ext.asyncio  import AsyncSession
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from models import remote
from sqlalchemy.exc import IntegrityError, NoResultFound
from litestar.exceptions import ClientException, NotFoundException,InternalServerException
from litestar.status_codes import HTTP_409_CONFLICT
from typing import TypeVar
from typing import List
from icecream import ic

initialize_config_dir(version_base=None, config_dir=f"{os.getcwd()}/config", job_name="demo")
cfg = compose(config_name="config")
lstate=State({"appcfg": cfg})
T = TypeVar('T')

def decrpyt_password(cybeark_string):
    p = subprocess.Popen(cybeark_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, _ = p.communicate()
    output_str = output.decode('utf-8')
    if "ERROR" in output_str.upper():
        print("Error")
        exit(1)
    return output_str.strip()

def get_engine(cfg: DictConfig, key: str)  -> Engine:
    connection_string = cfg[key].vendor.connection_string
    if key != "local":
        password=decrpyt_password(cfg[key].password_command)
        connection_string = connection_string.replace("REPLACEME", password)
    engine = create_engine(connection_string, echo=False)
    return engine

def get_connection_strnig(key: str)  -> str:
    connection_string = cfg[key].vendor.connection_string
    if key != "local":
        password=decrpyt_password(cfg[key].password_command)
        connection_string = connection_string.replace("REPLACEME", password)
    return connection_string

def generate_dates(start_date, end_date):
    delta = end_date - start_date
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        yield date


def reset_queue(q):
    q = queue.Queue()
    for i in range(10000):
        q.put(i+1)
    return q

async def on_startup():
    logger.debug("App started!")
    #logger.debug("That's it, beautiful and simple Started!")

sessionmaker = async_sessionmaker(expire_on_commit=False)
async def get_todo_list(done: Optional[bool], session: AsyncSession) -> list[remote.User]:
    query = select(remote.User)
    print(query)
    async with sessionmaker(bind=session.owner.state.engine) as session1:
        try:
            async with session1.begin():
                result = await session1.exec(query)
        except IntegrityError as exc:
            raise ClientException(
                status_code=HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc
    return result.scalars().all()


async def get_todo_listX(done: Optional[bool], session: AsyncSession) -> list[remote.User]:
    query = select(remote.User).order_by(remote.User.id.asc()).offset(0).limit(5)
    if done is not None:
        query = query.where(remote.User > 1)

    result = await session.exec(query)
    return result.scalars().all()

async def get_all_users(session: AsyncSession) -> list[remote.User]:
   return session.exec(select(remote.User)).scalars().all()



async def getemoji() -> str:
    fake = Faker()
    return fake.emoji()


def list_to_string(lst : List[str]) -> str:
    if not lst or len(lst) == 0:
        return ''
    return ','.join(str(item) for item in lst)

def string_to_list(string: str) -> List[str]:
    if not string or len(string) == 0:
        return []
    if ',' not in string:
        return [string]
    return string.split(',')

def list_to_json(lst : List[str]) -> str:
    if not lst or len(lst) == 0:
        return '[]'
    return json.dumps(lst)

def json_to_list(string: str) -> List[str]:
    if not string or len(string) == 0:
        return []
    return json.loads(string)


def check_three_vars(a, b, c):
    if (a == '' and b == '' and (c == '' or c == 'Select Date Format') ) or (a != '' and b != '' and (c != 'Select Date Format' or c!= ''))  :
        return True
    else:
        return False
    
def get_extra_rows_using_hash(df1, df2):
    """
    Compare two dataframes and return extra rows based on the 'ConcatenatedKeys' column.

    Parameters:
    df1 (pandas.DataFrame): First dataframe.
    df2 (pandas.DataFrame): Second dataframe.

    Returns:
    tuple: A tuple containing two dataframes with extra rows from df1 and df2.
    """
    df1_extra = df1[~df1["ConcatenatedKeys"].isin(df2["ConcatenatedKeys"])]
    df2_extra = df2[~df2["ConcatenatedKeys"].isin(df1["ConcatenatedKeys"])]

    return df1_extra, df2_extra

def get_common_rows(df1, df2, keys):
    """
    Get the common rows between two dataframes based on specified keys.

    Parameters:
    df1 (pandas.DataFrame): First dataframe.
    df2 (pandas.DataFrame): Second dataframe.
    keys (list): List of column names to use as keys for comparison.

    Returns:
    pandas.DataFrame: A dataframe containing the common rows between df1 and df2.
    """
    # Merge the dataframes on the specified keys
    merged_df = pd.merge(df1, df2, on=keys)

    return merged_df

def compare_mismatched_rows(
    df1: pd.DataFrame, df2: pd.DataFrame, mismatched_df: pd.DataFrame
) -> benedict[str, benedict]:
    """
    Compare mismatched rows in two dataframes and return a dictionary with differing columns.

    Parameters:
    df1 (pandas.DataFrame): First dataframe.
    df2 (pandas.DataFrame): Second dataframe.
    mismatched_df (pandas.DataFrame): Dataframe with mismatched rows.

    Returns:
    benedict: A dictionary with column names as keys and dataframes as values.
    """
    result_dict = benedict()

    for index, row in mismatched_df.iterrows():
        keys = row["ConcatenatedKeys"]
        df1_row = df1[df1["ConcatenatedKeys"] == keys]
        df2_row = df2[df2["ConcatenatedKeys"] == keys]

        for column in df1.columns:
            if (
                column not in ["ConcatenatedKeys", "HashValue"]
                and df1_row[column].values[0] != df2_row[column].values[0]
            ):
                if column not in result_dict:
                    result_dict[column] = []
                result_dict[column].append(
                    {
                        "df1_value": df1_row[column].values[0],
                        "df2_value": df2_row[column].values[0],
                        "ConcatenatedKeys": keys,
                    }
                )

    return result_dict

def find_mismatched_rows(df1, df2):
    """
    Compare two dataframes based on the 'ConcatenatedKeys' column and return rows where 'HashValue' is not matching.

    Parameters:
    df1 (pandas.DataFrame): First dataframe.
    df2 (pandas.DataFrame): Second dataframe.

    Returns:
    pandas.DataFrame: A dataframe containing the rows where 'HashValue' is not matching based on the 'ConcatenatedKeys' column.
    """
    merged_df = pd.merge(df1, df2, on="ConcatenatedKeys", suffixes=("_df1", "_df2"))
    ic(merged_df)
    mismatched_df = merged_df[merged_df["HashValue_df1"] != merged_df["HashValue_df2"]]
    ic(mismatched_df)
    return mismatched_df
