from datetime import date
from typing import Optional
import pendulum
from pydantic import BaseModel, computed_field, validator
from sqlmodel import SQLModel, Field, select
from enum import Enum
from lib.util import string_to_list, json_to_list
from icecream import ic
from sqlalchemy.ext.asyncio  import AsyncSession

class TComareTypes(Enum):
    NOT_SELECTED = "Select Comparison Type for This section"
    FILE = "File"
    TABLE = "Table"
    CTE = "CTE"

class TComareTypes(Enum):
    NOT_SELECTED = "Select Comparison Type for This section"
    FILE = "File"
    TABLE = "Table"
    CTE = "CTE"

    
class TCompareModel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    config_name: str = Field(default='', description="config name")
    left_obj_type: str = Field(default=TComareTypes.NOT_SELECTED.value, description="The leftype value")
    right_obj_type: str = Field(default=TComareTypes.NOT_SELECTED.value, description="The rigthtype value")
    left_db: str = Field(default='', description="Left DB")
    right_db: str = Field(default='', description="Right DB")
    left_tbl: str = Field(default='', description="Left Table")
    right_tbl: str = Field(default='', description="Right Table")
    left_pivot_choice : str = Field(default='', description="Left Pivot Choice")
    right_pivot_choice: str = Field(default='', description="Right Pivot Choice")
    left_pivot_column : str = Field(default='', description="Left Pivot Column")
    right_pivot_column: str = Field(default='', description="Right Pivot Column")
    left_pivot_format : str = Field(default='', description="Left Pivot format")
    right_pivot_format: str = Field(default='', description="Right Pivot format")
    left_where_clause : str = Field(default='', description="Left Where Clause")
    right_where_clause: str = Field(default='', description="Right Where Clause")
    left_custom_pivot_value : Optional[date] = Field(default=None, description="Left Custom Pivot Value")
    right_custom_pivot_value: Optional[date] = Field(default=None, description="Right Custom Pivot Value")


    common_columns_str : str = Field(default='', description="Common Columns")
    columns_to_exclude_str : str = Field(default='', description="Columns to Exclude")

    unique_columns_str : str = Field(default='', description="Unique Columns")

    @computed_field
    @property
    def common_columns(self) -> Optional[list[str]]:
        return json_to_list(self.common_columns_str)
    
    @computed_field
    @property
    def columns_to_exclude(self) -> Optional[list[str]]:
        return json_to_list(self.columns_to_exclude_str)
    
    @computed_field
    @property
    def unique_columns(self) -> Optional[list[str]]:
        return json_to_list(self.unique_columns_str)
    
    @validator('left_obj_type')
    def validate_left_obj_type(cls, value):
        if value.lower().startswith('select'):
            raise ValueError("⚠️Left Object Type is not selected⚠️")
        return value
    
    @validator('right_obj_type')
    def validate_right_obj_type(cls, value):
        if value.lower().startswith('select'):
            raise ValueError("⚠️Right Object Type is not selected⚠️")
        return value
    
    @validator('config_name')
    def validate_config_name(cls, value):
        if value == '':
            raise ValueError("⚠️Config Name is not provided, either select existing config or type new name to save it⚠️")
        return value
    
    @validator('left_db')
    def validate_left_db(cls, value):
        if value.lower().startswith('select') or value == '':
            raise ValueError("⚠️Left Database is not selected⚠️")
        return value
    
    @validator('right_db')
    def validate_right_db(cls, value):
        if value.lower().startswith('select') or value == '':
            raise ValueError("⚠️Right Database is not selected⚠️")
        ic(value)
        return value
    
    @validator('left_tbl')
    def validate_left_tbl(cls, value):
        if value.lower().startswith('select') or value == '':
            raise ValueError("⚠️Left Table is not selected⚠️")
        return value
    
    @validator('right_tbl')
    def validate_right_tbl(cls, value):
        if value.lower().startswith('select') or value == '':
            raise ValueError("⚠️Right Table is not selected⚠️")
        return value
    


async def get_configs(transaction_local: AsyncSession):
    result = await  transaction_local.execute(select(TCompareModel.config_name))
    return result.all()

async def get_configs_by_conifg_name(transaction_local: AsyncSession, config_name: str):
    #result = await  transaction_local.execute(select(TCompareModel).where(TCompareModel.config_name == config_name))
    x = await  transaction_local.execute(select(TCompareModel).where(TCompareModel.config_name == config_name))
    return x.scalars().all()
    #return result.fetchall()