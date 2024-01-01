from decimal import Decimal
from sqlmodel import Field, SQLModel
from datetime import datetime, date

class DenormalizedOrder(SQLModel, table=True):
    id: int = Field(primary_key=True)
    user_id: int = Field()
    user_name: str = Field()
    user_email: str = Field()
    product_id: int = Field()
    product_name: str = Field()
    batch_id: int = Field()
    batch_date: date = Field()
    start_time: datetime = Field()
    end_time: datetime = Field()
    total_price: Decimal = Field()