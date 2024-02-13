from datetime import date, datetime
from typing import Optional
import pendulum
from pydantic import BaseModel, computed_field, validator
from sqlmodel import Relationship, SQLModel, Field, select
from enum import Enum
from lib.util import string_to_list, json_to_list
from icecream import ic
# from sqlalchemy.ext.asyncio  import AsyncSession
from sqlmodel.ext.asyncio.session import AsyncSession

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



def CreateKeyTable(tablename) -> SQLModel:
    class KeyTable(SQLModel, table=True,extend_existing=True):
        __tablename__ = tablename
        __table_args__ = {'extend_existing': True}
        ConcatenatedKeys: str = Field(description="ConcatenatedKeys", primary_key=True)
    return KeyTable


  


#Tcomparemodel
    
class Tcomparemodel(SQLModel, table=True):
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
    tschduledcomparerunmodels: list["Tschduledcomparerunmodel"] = Relationship(back_populates="tcomparemodel")
    left_cte: str = Field(default='', description="Left CTE")
    right_cte: str = Field(default='', description="Right CTE")
    left_filename: str = Field(default='', description="Left Filename")
    right_filename: str = Field(default='', description="Right Filename")
    left_fdt_placeholder: str = Field(default='', description="Left File Date Placeholder")
    right_fdt_placeholder: str = Field(default='', description="Right File Date Placeholder")
    left_fdt_format: str = Field(default='', description="Left File Date Format")
    right_fdt_format: str = Field(default='', description="Right File Date Format")
    left_file_delimiter: str = Field(default='', description="Left File Delimiter")
    right_file_delimiter: str = Field(default='', description="Right File Delimiter")
    left_file_header_delimiter: str = Field(default='', description="Left File Header Delimiter")
    right_file_header_delimiter: str = Field(default='', description="Right File Header Delimiter")
    left_header_lines: int = Field(default=0, description="Left Header Lines")
    right_header_lines: int = Field(default=0, description="Right Header Lines")
    left_footer_lines: int = Field(default=0, description="Left Footer Lines")
    right_footer_lines: int = Field(default=0, description="Right Footer Lines")
    left_trailer_string: str = Field(default='', description="Left Trailer String")
    right_trailer_string: str = Field(default='', description="Right Trailer String")
    left_fixed_width: str = Field(default='', description="Left Fixed Width")
    right_fixed_width: str = Field(default='', description="Right Fixed Width")    
    left_file_columns: str = Field(default='', description="Left File Columns")
    right_file_columns: str = Field(default='', description="Right File Columns")
    common_columns_str : str = Field(default='', description="Common Columns")
    columns_to_exclude_str : str = Field(default='', description="Columns to Exclude")
    unique_columns_str : str = Field(default='', description="Unique Columns")
    created_by: str = Field(default='Litestar', description="Created By")
    created_on: datetime = Field(default=pendulum.now("America/Toronto"), description="Created On")
    updated_by: str = Field(default='Litestar', description="Updated By")
    updated_on: datetime = Field(default=pendulum.now("America/Toronto"), description="Updated On")

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
    

    @validator('id', pre=True)
    def blank_string(cls, value):
        if value == "" or value == "None":
            return None
        return value
    
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
        #ic(value)
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
    
#Tschduledcomparerunmodel
class Tschduledcomparerunmodel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_stat_time: Optional[datetime] = Field(default=None, description="Run Start Date")
    run_end_time: Optional[datetime] = Field(default=None, description="Run End Date")
    run_status: Optional[str] = Field(default=None, description="Run Status")
    run_error: Optional[str] = Field(default=None, description="Run Error")
    run_report: Optional[str] = Field(default=None, description="Run Report")
    tcomparemodel_id : int = Field(default=None, foreign_key="tcomparemodel.id")
    tcomparemodel: Optional[Tcomparemodel] = Relationship(back_populates="tschduledcomparerunmodels")
    created_by: str = Field(default='Litestar', description="Created By")
    created_on: datetime = Field(default=pendulum.now("America/Toronto"), description="Created On")


async def get_configs(transaction_local: AsyncSession):
    result = await  transaction_local.exec(select(Tcomparemodel.config_name))
    return result.all()

async def get_configs_by_conifg_name(transaction_local: AsyncSession, config_name: str):
    #result = await  transaction_local.exec(select(Tcomparemodel).where(Tcomparemodel.config_name == config_name))
    result = await  transaction_local.exec(select(Tcomparemodel).where(Tcomparemodel.config_name == config_name))
    return result.all()
    #return result.fetchall()