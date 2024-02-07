from decimal import Decimal
from typing import Optional, Union
from datetime import date, datetime, timedelta
from pydantic import computed_field
from sqlalchemy import (
    DECIMAL,
    Column,
    Index,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlmodel import Field, SQLModel, Relationship
from sqlalchemy.exc import IntegrityError, InvalidRequestError


class SearchAndOrder(SQLModel, table=False):
    page_number : Optional[int] = 1
    page_size : Optional[int] = 33
    order_list : Optional[list[Union[int, str]]] = [0,0]
    search_query : Optional[str] = ""

    @computed_field
    @property
    def order_sequnce(self) -> Optional[list[int]]:
        if len(self.order_list) > 0:
            if len(self.order_list) % 2 != 0:
                self.order_list.pop()
            return [int(item) for item in self.order_list[::2]]
        else:
            return [0]
        

    @computed_field
    @property
    def order_direction(self) -> Optional[list[int]]:
        if len(self.order_list) > 0:
            if len(self.order_list) % 2 != 0:
                self.order_list.pop()
            return [int(item) for item in self.order_list[1::2]]
        else:
            return [0]





class BaseHistory(SQLModel, table=False):
    def get_sql_column_sequnce() -> list[str]:
        return ["id"]
    @staticmethod
    def get_user_page_squeunce() -> list[int]:
        return [0]

    @staticmethod
    def get_sql_order_sequnce() -> list[int]:
        return [0]

    @staticmethod
    def get_sql_order_direction() -> list[int]:
        return [0]

    @staticmethod
    def get_sql_valid_search_columns() -> list[str]:
        return []
    

    @staticmethod
    def get_select_clause() -> list[str]:
        return """
            with result_cte as (
                    SELECT  [id]
                        ,[name]
                        ,[email]
                        ,[password]
                        ,[phone_number]
                        ,[address]
                        ,[date_of_birth]
                    FROM [user]
            )
        """
    
    @computed_field
    @property
    def order_list(self) -> Optional[list[int]]:
        combined_list = list(map(lambda x, y: [x, y], self.get_sql_order_sequnce(), self.get_sql_order_direction()))
        flattened_list = [item for sublist in combined_list for item in sublist]
        return flattened_list
    
# class UserHistory(SQLModel, table=False):
#     id: Optional[int]
#     name: Optional[str]
#     email: Optional[str]
#     address: Optional[str]
#     date_of_birth: Optional[date]
#     phone_number: Optional[str]

#     def sql_column_sequnce() -> list[str]:
#         return ["id", "name", "email", "address", "date_of_birth", "phone_number"]

#     def user_page_squeunce() -> list[int]:
#         return [0, 1, 2, 3, 4, 5]

#     def sql_order_sequnce() -> list[int]:
#         return [0,1,2,3]

#     def sql_order_direction() -> list[int]:
#         return [0,1,0,0]

#     def sql_valid_search_columns() -> list[str]:
#         return [1, 2]

#     def get_select_clause() -> list[str]:
#         return """
#             with result_cte as (
#                     SELECT  [id]
#                         ,[name]
#                         ,[email]
#                         ,[password]
#                         ,[phone_number]
#                         ,[address]
#                         ,[date_of_birth]
#                     FROM [user]
#             )
#         """

class StatuSHistory(BaseHistory, table=False):
    process_id: Optional[int] = -1
    process_status: Optional[str] = ""

    
class UserHistory(BaseHistory, table=False):
    id: Optional[int] = -1
    name: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[str] = ""
    date_of_birth: Optional[date] = ""
    phone_number: Optional[str] = ""

    @staticmethod
    def get_sql_column_sequnce() -> list[str]:
        return ["id", "name", "email", "address", "date_of_birth", "phone_number"]
    @staticmethod
    def get_user_page_squeunce() -> list[int]:
        return [0, 1, 2, 3, 4, 5]
    @staticmethod
    def get_sql_order_sequnce() -> list[int]:
        return [1,2,3]
    @staticmethod
    def get_sql_order_direction() -> list[int]:
        return [0,0,0]
    @staticmethod
    def get_sql_valid_search_columns() -> list[str]:
        return [1, 2,3]
    @staticmethod
    def get_select_clause() -> list[str]:
        return """
            with result_cte as (
                    SELECT  [id]
                        ,[name]
                        ,[email]
                        ,[password]
                        ,[phone_number]
                        ,[address]
                        ,[date_of_birth]
                    FROM [user]
            )
        """

   