from litestar import Controller, get
from litestar.response import ServerSentEvent
from litestar.response import Template
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation
from collections.abc import AsyncGenerator
from random import randint
import asyncio
#from sqlalchemy.ext.asyncio  import AsyncSession
from sqlmodel.ext.asyncio.session import AsyncSession
from lib.util import getemoji


async def my_generator() -> AsyncGenerator[bytes, None]:
    count = 0
    while count < randint(5, 15):
        try:
            if count % 2 == 0:
                await asyncio.sleep(0.12)
            else:
                await asyncio.sleep(1.2)
        except asyncio.CancelledError:
            break
        count += 1
        yield str(f'yyyy{count}{await getemoji()}')


async def my_generator2() -> AsyncGenerator[bytes, None]:
    count = 0
    while count < randint(5, 15):
        try:
            if count % 2 == 0:
                await asyncio.sleep(0.12)
            else:
                await asyncio.sleep(1.2)
        except asyncio.CancelledError:
            break
        count += 1
        yield str(f'xxxx{count}{await getemoji()}')

class ssetController(Controller):
    @get(path="/count", sync_to_thread=False)
    def sse_handler() -> ServerSentEvent:
        return ServerSentEvent(my_generator(),event_id='vovo',event_type='vivi',retry_duration=120000)

    @get(path="/count2", sync_to_thread=False)
    def sse_handler2() -> ServerSentEvent:
        return ServerSentEvent(my_generator2(),event_id='vovo',event_type='ssss',retry_duration=6000)


    @get(path="/sset", sync_to_thread=False)
    def sset() -> Template:
        context = {'title':'sse test'}
        return HTMXTemplate(
            template_name='sset.html', context=context, push_url='/sset'
        )
