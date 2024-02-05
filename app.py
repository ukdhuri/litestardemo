
import signal
from typing import TYPE_CHECKING
from litestar import Controller, Litestar, Request, Response, Router, WebSocket, get, websocket
import os
from datetime import datetime, time, timedelta
from litestar import app
from loguru import logger

import controllers
import controllers.Home
import controllers.Status
import controllers.Factory
import controllers.SocketWeb
import controllers.Compt
from lib.dependencies import (
    db_remote,
    db_local,
    provide_transaction_remote,
    provide_transaction_remote,
    provide_transaction_local,
)
from lib.logging import logging_config
from lib.scheduler import getemoji, scheduled_task, start_scheduler, stop_scheduler
from lib.util import on_startup
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
from pathlib import Path
from litestar.contrib.htmx.request import HTMXRequest
from models import remote
from sqlmodel import select
from lib.stores import stores
import lib.exception as exception
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar.static_files.config import StaticFilesConfig
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from pathlib import Path
from litestar import get
from litestar.response import File
import asyncio
from apscheduler.schedulers.asyncio   import AsyncIOScheduler
from litestar import Litestar, get
from litestar.response import Template
from lib.stores import before_shutdown
from litestar.middleware.session.server_side import ServerSideSessionConfig


if TYPE_CHECKING:
    from litestar.types import ASGIApp, Receive, Scope, Send

async def on_startup():
    await start_scheduler(app)

async def on_shutdown():
    await before_shutdown()
    await stop_scheduler(app)

@get(["/bar"], sync_to_thread=False)
def shutdown() -> str:
    os.kill(os.getpid(), signal.SIGTERM)
    return "Shutting down" 


@get(path="/favicon.ico", media_type="image/x-icon")
def handle_file_download() -> File:
    return File(
        path=Path(Path(__file__).resolve().parent,"static" ,"favicon").with_suffix(".ico"),
        filename="favicon.ico",
    )
 
@get(["/testwebsocket"], sync_to_thread=False)
async def testwebsocket(request: Request, channels: ChannelsPlugin) -> Response:
    if 'publisher_started' in app.state:
        return Response[None]
    emo = await getemoji()
    #[channels.publish(await getemoji(), channels=[chnl]) for chnl in request.app.dependencies['channels'].value._channels]
    scheduler = AsyncIOScheduler()
    #scheduler.add_job(scheduled_task, "interval", seconds=5, args=[app,channels])
    scheduler.start()
    app.state['scheduler'] = scheduler
    app.state['publisher_started']  = True
    app.state['schedulerrequest']  = request
    return Response[True]



app = Litestar(

    [controllers.Compt.ComptController, testwebsocket,handle_file_download,shutdown,controllers.Home.HomeController,controllers.Factory.FactoryController,controllers.SocketWeb.SocketWebController],
    static_files_config=[
        StaticFilesConfig(directories=["static"], path="/static",send_as_attachment=True,),
        
    ],
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
    middleware=[ServerSideSessionConfig().middleware],
    stores=stores,
    plugins=[ChannelsPlugin(channels=["emoji"],backend=MemoryChannelsBackend(history=10000),arbitrary_channels_allowed=True,create_ws_route_handlers=True,ws_handler_send_history=10000)],
)






#backend.add_job(my_async_function, 'interval', seconds=10, args=[provide_transaction_remote])
#

# logger.debug('👌👌👌👌😊😊😊😊😊')
# scheduler.add_job(my_async_function, 'interval', seconds=10, args=[provide_transaction_remote])
# scheduler.start()
