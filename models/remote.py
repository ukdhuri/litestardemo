from decimal import Decimal
from typing import Optional
from datetime import date,datetime,timedelta
from pydantic import computed_field
from sqlalchemy import DECIMAL, Column, Index, PrimaryKeyConstraint, String, UniqueConstraint
from sqlmodel import Field, SQLModel, Relationship
from sqlalchemy.exc import IntegrityError,InvalidRequestError

# class MytableX(SQLModel, table=True):
#     __table_args__ = (UniqueConstraint("kocha", "column2", name="uc1t")), (UniqueConstraint("id3", "column3", name="uc3t")) , (Index('hhh_id4_clm5', 'id4', 'bbbb')),
#     id1: Optional[int] = Field(default=None,primary_key=True)
#     kocha: int = Field(index=True)
#     id3: int = Field(unique=True)
#     id4: int = Field(index=True)
#     column1: str = Field(sa_column=Column(String(10)))
#     column2: str = Field(sa_column=Column(String(10)))
#     column3: str = Field(sa_column=Column(String(10)))
#     column4: str = Field(sa_column=Column(String(10)))
#     bbbb: str = Field(sa_column=Column(String(10)))
#     money: Decimal = Field(default=0, max_digits=5, decimal_places=3)
#     price_column: Decimal = Field(sa_column=Column(DECIMAL(precision=10, scale=2)))
#     age: Optional[int] = Field(default=None, index=True)



def is_pydantic(obj: object):
    """Checks whether an object is pydantic."""
    return type(obj).__class__.__name__ == "ModelMetaclass"


def parse_pydantic_schema(schema):
    """
        Iterates through pydantic schema and parses nested schemas
        to a dictionary containing SQLAlchemy models.
        Only works if nested schemas have specified the Meta.orm_model.
    """
    parsed_schema = dict(schema)
    for key, value in parsed_schema.items():
        try:
            if isinstance(value, list) and len(value):
                if is_pydantic(value[0]):
                    parsed_schema[key] = [schema.Meta.orm_model(**schema.dict()) for schema in value]
            else:
                if is_pydantic(value):
                    parsed_schema[key] = value.Meta.orm_model(**value.dict())
        except AttributeError:
            raise AttributeError("Found nested Pydantic model but Meta.orm_model was not specified.")
    return parsed_schema

class OrderResult(SQLModel, table=False):
    id: Optional[int]
    start_time: datetime
    end_time: datetime
    batch_date: Optional[date]
    name: Optional[str]

class User(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("email", name="email_uc")),
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field()
    #email: str = Field(unique=True)
    email: str = Field(sa_column=Column(String(300)))
    password: str = Field()
    phone_number: str = Field()
    address: str = Field()
    date_of_birth: Optional[date] = Field(default=None)
    orders: list["Order"] = Relationship(back_populates="user")

class Batch(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("batch_date","status")),
    id: Optional[int] = Field(default=None, primary_key=True)
    batch_date: date = Field()
    status: str = Field(sa_column=Column(String(10)))
    start_time: datetime = Field()
    end_time: datetime = Field()
    orders: list["Order"] = Relationship(back_populates="batch")

class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    batch_id: int = Field(foreign_key="batch.id")
    #total_price: Decimal = Field(default=0, max_digits=15, decimal_places=2)
    orderitems: list["OrderItem"] = Relationship(back_populates="order")
    user: User = Relationship(back_populates="orders")
    batch: Batch = Relationship(back_populates="orders")
    start_time: Optional[datetime] = Field(default=datetime.now())
    end_time: Optional[datetime] = Field(default=None)
    @computed_field
    @property
    def total_price(self) -> Optional[Decimal]:
        try :
            if self.orderitems:
                return sum(item.price for item in self.order_items)
        except InvalidRequestError as e:
            return 0
        
    
    # @computed_field
    # @property
    # def end_time(self) -> Optional[datetime]:
    #     if self.start_time:
    #         return self.start_time + timedelta(seconds=random.randint(20, 120))


class ProductCategoryLink(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    # product_id: int = Field(foreign_key="product.id", primary_key=True)
    # category_id: int = Field(foreign_key="category.id", primary_key=True)
    product_id: int = Field(foreign_key="product.id")
    category_id: int = Field(foreign_key="category.id")
    product: "Product" = Relationship(back_populates="category_links")
    category: "Category" = Relationship(back_populates="product_links")

class Product(SQLModel, table=True):
    __table_args__ = (Index('idx_name',"name")),
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=Column(String(50)))
    description: str = Field()
    price: Decimal = Field(default=0, max_digits=15, decimal_places=2)
    category_links: Optional[list[ProductCategoryLink]] = Relationship(back_populates="product")
    orderitem: "OrderItem" = Relationship(back_populates="product")

class Category(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field()
    product_links: list[ProductCategoryLink] = Relationship(back_populates="category")
    #products: list[Product] = Relationship(back_populates="category")

class OrderItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int = Field(foreign_key="order.id")
    product_id: int = Field(foreign_key="product.id")
    #order_id: Optional[int] = Field()
    #product_id: Optional[int] = Field()
    quantity: int = Field(default=1)
    #price: Decimal = Field(default=0, max_digits=15, decimal_places=2)
    order: Order = Relationship(back_populates="orderitems")
    product: Product = Relationship(back_populates="orderitem")
    @computed_field
    @property
    def price(self) -> Optional[Decimal]:
        return self.product.price * self.quantity


