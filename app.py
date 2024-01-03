import signal
from typing import TYPE_CHECKING
from typing import Optional
from hydra import compose, initialize
import logging
import sys
from litestar import Controller, Litestar, Response, Router, get
import os
from datetime import datetime, time, timedelta
from litestar import app
from loguru import logger

import controllers
import controllers.Home
import controllers.Status
import controllers.Factory
from lib.dependencies import (
    db_remote,
    db_local,
    provide_transaction_remote,
    provide_transaction_remote,
    provide_transaction_local,
)
from lib.logging import logging_config
from lib.scheduler import start_scheduler, stop_scheduler
from lib.service import get_product_fn
from lib.util import on_startup, lstate
from litestar.datastructures import State
from models.local import DenormalizedOrder
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
from pathlib import Path
from litestar.contrib.htmx.request import HTMXRequest
from litestar.contrib.htmx.response import HTMXTemplate
from sqlalchemy.ext.asyncio import AsyncSession
from litestar.response import Template
from models import remote
from sqlmodel import select
from lib.stores import stores
import lib.exception as exception
from litestar.exceptions import HTTPException, ValidationException
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR
# xxxst.get("foo", renew_for=1)
from apscheduler.schedulers.asyncio   import AsyncIOScheduler
from collections.abc import AsyncGenerator
from litestar.types import ASGIApp, Scope, Receive, Send
from litestar.middleware import MiddlewareProtocol
import asyncio


if TYPE_CHECKING:
    from litestar.types import ASGIApp, Receive, Scope, Send


async def on_startup():
    await start_scheduler(app)

async def on_shutdown():
    await stop_scheduler(app)

@get(["/shutdown"], sync_to_thread=False)
def shutdown() -> str:
    os.kill(os.getpid(), signal.SIGTERM)
    return "Shutting down" 


app = Litestar(
    [shutdown,controllers.Home.HomeController, controllers.Factory.FactoryController],
    exception_handlers=exception.exception_handlers,
    request_class=HTMXRequest,
    dependencies={
        "transaction_remote": provide_transaction_remote,
        "transaction_local": provide_transaction_local,
    },
    lifespan=[db_remote, db_local],
    logging_config=logging_config,
    on_startup=[on_startup],on_shutdown=[on_shutdown],
    template_config=TemplateConfig(
        directory=Path("templates"),
        engine=JinjaTemplateEngine,
    ),
    debug=True,
    stores=stores,

)


# logger.debug('👌👌👌👌😊😊😊😊😊')
# scheduler.add_job(my_async_function, 'interval', seconds=10, args=[provide_transaction_remote])
# scheduler.start()
