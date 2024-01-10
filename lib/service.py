from typing import Optional
from litestar import Request
from loguru import logger
import pandas as pd
from sqlalchemy import text
from sqlmodel import or_, select
from lib.util import T
from models import remote
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, and_
from datetime import date
import copy
from typing import TypeVar
from models.History import BaseHistory, SearchAndOrder
import static.SConstants
from litestar.response import Template

def read_sql_query(con, stmt):
    return pd.read_sql(stmt, con)

async def get_df(stmt, engine):
    data = pd.read_sql(stmt, engine)
    return data


async def build_context(title: str, resultlist: list[BaseHistory], hist: BaseHistory,data: SearchAndOrder,update_order_section = False,shouldusepagination = False) -> dict:
    return  {
        "title": title,
        "result_list": resultlist,
        "user_page_squeunce": hist.get_user_page_squeunce(),
        "sql_column_sequnce": hist.get_sql_column_sequnce(),
        "sql_order_sequnce": data.order_sequnce,
        "sql_order_direction": data.order_direction,
        "clm_name_mapping" : static.SConstants.clm_name_mapping,
        "rev_direction": static.SConstants.rev_direction,
        "update_order_section"  : update_order_section
    }
async def get_historical_result(
    request: Request,
    asyncsession: AsyncSession,
    scalar: bool = False,
    dilect : str = static.SConstants.Dilects.TSQL.name,
    tracker: Optional[SearchAndOrder] = None,
    histmodel: Optional[BaseHistory] = None
    ):
        context = {
            'cte_select_statment':histmodel.get_select_clause(),
            'sql_column_sequnce':histmodel.get_sql_column_sequnce(),
            'sql_order_sequnce': tracker.order_sequnce,
            'sql_order_direction':tracker.order_direction,
            'direction_dict': static.SConstants.direction,
            'valid_search_columns': histmodel.get_sql_valid_search_columns(),
            'search_query': tracker.search_query,
            'page_number': tracker.page_number,
            'page_size': tracker.page_size,
            'dilect' : dilect
        }
        sql_statement = Template(template_name="sql/history.txt",context=context).to_asgi_response(request.app,request).body.decode("utf-8")
        sql_statement = str(sql_statement)
        print(sql_statement)
        result = await asyncsession.execute(text(str(sql_statement)))
        
        if scalar:
            res =  result.scalars().all()
        else:
            res =  result.all()
        return res
        #return [remote.OrderResult(**row._asdict()) for row in rows]





async def get_users_fn(
    transaction_remote: AsyncSession,
    search_query: str = "",
    page_number: int = 1,
    page_size: int = 33,
    scalar=False,
    order_by_column: str = "id",
    order_direction: str = "asc",
) -> list[remote.User]:
    query = select(remote.User)
    if search_query:
        query = query.filter(
            or_(
                remote.User.name.ilike(f"%{search_query}%"),
                remote.User.address.ilike(f"%{search_query}%"),
                remote.User.email.ilike(f"%{search_query}%"),
                # Add more columns as needed
            )
        )
    if page_size == -1:
        query = query.order_by(
            getattr(remote.User, order_by_column).asc()
            if order_direction == "asc"
            else getattr(remote.User, order_by_column).desc()
        )
    else:
        query = (
            query.order_by(
                getattr(remote.User, order_by_column).asc()
                if order_direction == "asc"
                else getattr(remote.User, order_by_column).desc()
            )
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )

    result = await transaction_remote.execute(query)
    if scalar:
        return result.scalars().all()
    else:
        return result.all()
    


async def get_recent_orders(
    transaction_remote: AsyncSession,
    search_query: str = "",
    page_number: int = 1,
    page_size: int = 33,
    scalar=False,
    order_by_column: str = "id",
    order_direction: str = "desc",
) -> list[remote.OrderResult]:
    batch = await get_batch_fn(transaction_remote, status="inprogress")
    ##different column scalr for html will not work
    query = (
        select(
            remote.Order.id,
            remote.Order.start_time,
            remote.Order.end_time,
            remote.Batch.batch_date,
            remote.User.name
        )
        .join(remote.Batch, remote.Order.batch_id == remote.Batch.id)
        .join(remote.User, remote.Order.user_id == remote.User.id)
        .where(remote.Order.batch_id == batch.id)
        .where(
            or_(
                remote.User.name.contains(search_query),
                remote.User.address.contains(search_query),
            )
        )
    )
    if page_size == -1:
        query = query.order_by(
            getattr(remote.Order, order_by_column).asc()
            if order_direction == "asc"
            else getattr(remote.Order, order_by_column).desc()
        )
    else:
        query = (
            query.order_by(
                getattr(remote.Order, order_by_column).asc()
                if order_direction == "asc"
                else getattr(remote.Order, order_by_column).desc()
            )
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )
    result = await transaction_remote.execute(query)
    if scalar:
        res = result.scalars().all()
        return res
    else:
        res = result.all()
        return res
    #return [remote.OrderResult(**row._asdict()) for row in rows]


async def last_order_id(transaction_remote: AsyncSession) -> int:
    batch = await get_batch_fn(transaction_remote, status="inprogress")
    query = (
        select(
            remote.Order.id,
            remote.Order.start_time,
            remote.Order.end_time,
            remote.Batch.batch_date,
            remote.User.name,
            remote.User.address,
        )
        .join(remote.Batch, remote.Order.batch_id == remote.Batch.id)
        .where(
            remote.Order.batch_id == batch.id
            if batch.id
            else remote.Batch.batch_date == batch.batch_date
        )
        .order_by(remote.Order.id.desc())
    )
    query = query.order_by(remote.User.id.asc()).limit(1)
    result = await transaction_remote.execute(query)
    orderid = result.scalar().first()
    return orderid


async def get_batch_fn(
    transaction_remote: AsyncSession,
    batch_id: Optional[int] = None,
    batch_date: Optional[date] = None,
    status: Optional[str] = None,
) -> remote.Batch:
    query = select(remote.Batch).where(
        and_(
            remote.Batch.id == batch_id if batch_id else True,
            remote.Batch.batch_date == batch_date if batch_date else True,
            remote.Batch.status == status if status else True,
        )
    )
    print(query)
    result = await transaction_remote.execute(query)
    return result.one()[0]


async def get_product_fn(
    transaction_remote: AsyncSession,
    search_query: str = "",
    page_number: int = 1,
    page_size: int = 12,
) -> list[remote.Product]:
    query = select(remote.Product)
    if search_query:
        query = query.filter(
            or_(
                remote.Product.name.ilike(f"%{search_query}%"),
                remote.Product.description.ilike(f"%{search_query}%"),
                # Add more columns as needed
            )
        )
    if page_size == -1:
        query = query.order_by(remote.Product.id.asc())
        print(query)
    else:
        query = (
            query.order_by(remote.Product.id.asc())
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )

    result = await transaction_remote.execute(query)
    return result.all()


async def get_order_fn(
    transaction_remote: AsyncSession,
    batch_id: Optional[int] = None,
    page_number: int = 1,
    page_size: int = 12,
) -> list[remote.Order]:
    query = select(remote.Order).where(
        and_(
            remote.Order.id == batch_id if batch_id else True,
        )
    )
    if page_size == -1:
        query = query.order_by(remote.Order.id.asc())
        print(query)
    else:
        query = (
            query.order_by(remote.Order.id.asc())
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )

    result = await transaction_remote.execute(query)
    return result.all()


async def upsert_record(transaction_remote: AsyncSession, record: T) -> T:
    transaction_remote.add(record)
    await transaction_remote.commit()
    return record


async def upsert_records(transaction_remote: AsyncSession, records: list[T]) -> list[T]:
    transaction_remote.add_all(records)
    await transaction_remote.commit()
    return records
