from contextlib import asynccontextmanager
from typing import Any, Optional
from collections.abc import AsyncGenerator
from loguru import logger
from sqlmodel import SQLModel, Field
from sqlalchemy import select
from models import local, remote
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.exc import IntegrityError, NoResultFound
import pandas as pd
from litestar import Litestar, get, post, put
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException,InternalServerException
from litestar.status_codes import HTTP_409_CONFLICT
from litestar import Litestar, Request, get
import static.SConstants 
from lib.util import cfg


@asynccontextmanager
async def db_remote(app: Litestar) -> AsyncGenerator[None, None]:
    engine_remote = getattr(app.state, "engine_remote", None)
    if engine_remote is None:
        constr = cfg.remote.vendor.connection_string.replace("REPLACEME", "2582")
        app.state.remote_con_str = constr
        engine_remote = create_async_engine(constr,echo=True)
        app.state.engine_remote = engine_remote
        app.state['remote_con_str'] = constr
    async with engine_remote.begin() as conn:
        #await conn.run_sync(SQLModel.metadata.create_all)
        logger.info('engine_remote created')
    try:
        yield
    finally:
        await engine_remote.dispose()


## as of now no use
@asynccontextmanager
async def db_remote1(app: Litestar) -> AsyncGenerator[None, None]:
    engine_remote = getattr(app.state, "engine_remote1", None)
    if engine_remote1 is None:
        constr = cfg.remote.vendor.connection_string.replace("REPLACEME", "2582")
        app.state.remote_con_str = constr
        engine_remote1 = create_async_engine(constr,echo=True)
        app.state.engine_remote1 = engine_remote1
        app.state['remote_con_str'] = constr
    async with engine_remote1.begin() as conn:
        #await conn.run_sync(SQLModel.metadata.create_all)
        logger.info('engine_remote1 created')
    try:
        yield
    finally:
        await engine_remote.dispose()

@asynccontextmanager
async def db_local(app: Litestar) -> AsyncGenerator[None, None]:
    engline_local = getattr(app.state, "engline_local", None)
    if engline_local is None:
        engline_local = create_async_engine(cfg.local.vendor.connection_string)
        app.state.engline_local = engline_local
        app.state['localcon_str'] = cfg.local.vendor.connection_string
    async with engline_local.begin() as conn:
        #await conn.run_sync(SQLModel.metadata.create_all)
        logger.info('engine_remote created')
    try:
        yield
    finally:
        await engline_local.dispose()


sessionmaker = async_sessionmaker(expire_on_commit=False)


async def provide_transaction_remote(state: State) -> AsyncGenerator[AsyncSession, None]:
    async with sessionmaker(bind=state.engine_remote) as session_remote:
        try:
            async with session_remote.begin():
                yield session_remote
        except IntegrityError as exc:
            raise ClientException(
                status_code=HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc

## as of now no use
async def provide_transaction_remote1(state: State) -> AsyncGenerator[AsyncSession, None]:
    async with sessionmaker(bind=state.engine_remote1) as session_remote1:
        try:
            async with session_remote1.begin():
                yield session_remote1
        except IntegrityError as exc:
            raise ClientException(
                status_code=HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc

async def provide_transaction_local(state: State) -> AsyncGenerator[AsyncSession, None]:
    async with sessionmaker(bind=state.engine_local) as session_local:
        try:
            async with session_local.begin():
                yield session_local
        except IntegrityError as exc:
            raise ClientException(
                status_code=HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc