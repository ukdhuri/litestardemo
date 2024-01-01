
from typing import Optional
from hydra import compose, initialize
import logging
import sys
from litestar import Controller, Litestar, Response, Router, get
import os
from datetime import datetime,time,timedelta
from litestar import app
from loguru import logger

import controllers
import controllers.Home
import controllers.Status
from lib.dependencies import  db_remote, db_local,provide_transaction_remote,provide_transaction_remote,provide_transaction_local
from lib.logging import logging_config
from lib.util import on_startup,lstate
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



@get(path="/preview")
async def preview(request: HTMXRequest) -> Template:
    htmx = request.htmx  #  if true will return HTMXDetails class object
    if htmx:
        print(htmx.current_url)
    # OR
    if request.htmx:
        print(request.htmx.current_url)
    return HTMXTemplate(template_name="indexh.html", context="", push_url="/form")


app = Litestar(
    [preview,controllers.Status.StatusController,controllers.Home.HomeController],
    request_class=HTMXRequest,
    dependencies={"transaction_remote": provide_transaction_remote,"transaction_local": provide_transaction_local},
    lifespan=[db_remote,db_local], logging_config=logging_config, on_startup=[on_startup],
    template_config=TemplateConfig(
        directory=Path("templates"),
        engine=JinjaTemplateEngine,
    ), debug=True
)   

#dependencies={"transaction": provide_transaction},lifespan=[db_remote,db_local],state=lstate,logging_config=logging_config, on_startup=[on_startup])

