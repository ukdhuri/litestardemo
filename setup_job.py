import queue
import random
from faker import Faker
from polyfactory.factories.pydantic_factory import ModelFactory
import pytz
import faker_commerce
from models import local, remote
from datetime import date,datetime,timedelta,time
from hydra import compose, initialize
from sqlmodel import SQLModel,Session, select
import pandas as pd
from sqlalchemy import func, text
from lib.util import get_engine
from factories.allf import *

if __name__ == "__main__":
    #initialize(version_base=None, config_path="config", job_name="demo")
    cfg = compose(config_name="config")
    # engine=get_engine(cfg, key="remote")
    # SQLModel.metadata.drop_all(engine)
    # #SQLModel.metadata.create_all(engine, tables=[all.User.__table__,all.Batch.__table__,all.Order.__table__,all.OrderItem.__table__,all.ProductCategoryLink.__table__,all.Product.__table__,all.Category.__table__])
    # SQLModel.metadata.create_all(engine)
    # produce_factory_object(engine)
    localengine=get_engine(cfg, key="local")
    SQLModel.metadata.drop_all(localengine)
    SQLModel.metadata.create_all(localengine, tables=[local.DenormalizedOrder.__table__])
    dnf=DenormalizedOrderFactory.batch(size=100)
    print(dnf)
    with Session(localengine) as session:
        session.add_all(dnf)
        session.commit()


    