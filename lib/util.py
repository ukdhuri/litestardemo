import json
import queue
import subprocess
from typing import Optional
from faker import Faker
from loguru import logger
from omegaconf import DictConfig
from sqlalchemy import Engine
from sqlmodel import create_engine, select
from datetime import datetime,date,time,timedelta
from litestar.datastructures import State
from hydra import compose, initialize_config_dir
from models import local
import os
from sqlalchemy.ext.asyncio  import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from models import remote
from sqlalchemy.exc import IntegrityError, NoResultFound
from litestar.exceptions import ClientException, NotFoundException,InternalServerException
from litestar.status_codes import HTTP_409_CONFLICT
from typing import TypeVar
from typing import List

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
                result = await session1.execute(query)
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

    result = await session.execute(query)
    return result.scalars().all()

async def get_all_users(session: AsyncSession) -> list[remote.User]:
   return session.execute(select(remote.User)).scalars().all()



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