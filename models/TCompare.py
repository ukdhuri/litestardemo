from sqlmodel import SQLModel, Field
from enum import Enum


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

    
class TCompareModel(SQLModel, table=False):
    left_obj_type: str = Field(default=TComareTypes.NOT_SELECTED.value, description="The leftype value")
    right_obj_type: str = Field(default=TComareTypes.NOT_SELECTED.value, description="The rigthtype value")
    selected_left_db: str = Field(default='', description="Left DB")
    selected_right_db: str = Field(default='', description="Right DB")
    selected_left_tbl: str = Field(default='', description="Left Table")
    selected_right_tbl: str = Field(default='', description="Right Table")

    