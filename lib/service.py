from typing import Optional
from loguru import logger
from sqlmodel import or_, select
from lib.util import T
from models import remote
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, and_
from datetime import date
import copy
from typing import TypeVar

async def get_users_fn(transaction_remote: AsyncSession, search_query: str = "", page_number: int = 1, page_size: int = 33,scalar=False)-> list[remote.User]:
    query=select(remote.User)
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
        query = query.order_by(remote.User.id.asc())
        print(query)
    else:
        query = query.order_by(remote.User.id.asc()).offset((page_number - 1) * page_size).limit(page_size)
    
    result = await transaction_remote.execute(query)
    if scalar:
        return result.scalars().all()
    else:
        return result.all()


async def get_recent_orders(transaction_remote: AsyncSession, search_query: str = "", page_number: int = 1, page_size: int = 33)-> list[remote.OrderResult]:
    batch = await get_batch_fn(transaction_remote, status="inprogress")
    query = select(
        remote.Order.id,
        remote.Order.start_time,
        remote.Order.end_time,
        remote.Batch.batch_date,
        remote.User.name
    ).join(
        remote.Batch,
        remote.Order.batch_id == remote.Batch.id
    ).join(
        remote.User,
        remote.Order.user_id == remote.User.id
    ).where(
        remote.Order.batch_id == batch.id
    ).where(
        or_(
            remote.User.name.contains(search_query),
            remote.User.address.contains(search_query)
        )
    ).order_by(
        remote.Order.id.desc()
    )
    if page_size == -1:
        query = query.order_by(remote.User.id.asc())
        print(query)
    else:
        query = query.order_by(remote.User.id.asc()).offset((page_number - 1) * page_size).limit(page_size)
    print(query)
    result = await transaction_remote.execute(query)
    fl = result.all()

    result = await transaction_remote.execute(query)
    rows = result.all()
    return [remote.OrderResult(**row._asdict()) for row in rows]


async def last_order_id(transaction_remote: AsyncSession)-> int:
    batch = await get_batch_fn(transaction_remote, status="inprogress")    
    query = select(
        remote.Order.id,
        remote.Order.start_time,
        remote.Order.end_time,
        remote.Batch.batch_date,
        remote.User.name,
        remote.User.address
    ).join(
        remote.Batch,
        remote.Order.batch_id == remote.Batch.id
    ).where(
        remote.Order.batch_id == batch.id if batch.id else remote.Batch.batch_date == batch.batch_date
    ).order_by(
        remote.Order.id.desc()
    )
    query = query.order_by(remote.User.id.asc()).limit(1)
    result = await transaction_remote.execute(query)
    orderid = result.scalar().first()
    return orderid
    

async def get_batch_fn(transaction_remote: AsyncSession, batch_id: Optional[int] = None, batch_date: Optional[date] = None, status: Optional[str] = None)-> remote.Batch:
    query = select(remote.Batch).where(
        and_(
            remote.Batch.id == batch_id if batch_id else True,
            remote.Batch.batch_date == batch_date if batch_date else True,
            remote.Batch.status == status if status else True
        )
    )
    print(query)
    result = await transaction_remote.execute(query)
    return result.one()[0]


async def get_product_fn(transaction_remote: AsyncSession, search_query: str = "", page_number: int = 1, page_size: int = 12)-> list[remote.Product]:
    query=select(remote.Product)
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
        query = query.order_by(remote.Product.id.asc()).offset((page_number - 1) * page_size).limit(page_size)
    
    result = await transaction_remote.execute(query)
    return result.all()

async def get_order_fn(transaction_remote: AsyncSession, batch_id: Optional[int] = None, page_number: int = 1, page_size: int = 12)-> list[remote.Order]:
    query=select(remote.Order).where(
        and_(
            remote.Order.id == batch_id if batch_id else True,
        )
    )   
    if page_size == -1:
        query = query.order_by(remote.Order.id.asc())
        print(query)
    else:
        query = query.order_by(remote.Order.id.asc()).offset((page_number - 1) * page_size).limit(page_size)
    
    result = await transaction_remote.execute(query)
    return result.all()


async def upsert_record(transaction_remote: AsyncSession, record: T) -> T:
    transaction_remote.add(record)
    await transaction_remote.commit()
    return record

async def upsert_records(transaction_remote: AsyncSession, records: list[T]) ->list[T]:
    transaction_remote.add_all(records)
    await transaction_remote.commit()
    return records