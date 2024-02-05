

from polyfactory.factories.pydantic_factory import ModelFactory
from hydra import compose
from sqlmodel import Field, SQLModel, create_engine
import pandas as pd
from lib.util import get_engine
from factories.allf import *
from models.TCompare import TCompareModel
from models.local import DenormalizedOrder

if __name__ == "__main__":
    cfg = compose(config_name="config")
    engine =create_engine( f"sqlite:///{cfg.local.sqlite_file_name}")
    SQLModel.metadata.drop_all(engine ) 
    SQLModel.metadata.create_all(engine, tables=[TCompareModel.__table__])


    