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
    