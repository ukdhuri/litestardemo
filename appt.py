from contextlib import asynccontextmanager
from typing import AsyncContextManager
from faker import Faker
from litestar import Litestar, Request, WebSocket, get
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar.exceptions import WebSocketDisconnect
from litestar.handlers import websocket_listener
from litestar.static_files import StaticFilesConfig







async def getemoji() -> str:
    fake = Faker()
    return fake.emoji()


from asyncio import sleep
from collections.abc import AsyncGenerator

from litestar import Litestar, get
from litestar.response import ServerSentEvent


async def my_generator() -> AsyncGenerator[bytes, None]:
    count = 0
    while count < 10:
        await sleep(3)
        count += 1
        yield str(count)


@get(path="/count", sync_to_thread=False)
def sse_handler() -> ServerSentEvent:
    return ServerSentEvent(my_generator())


app = Litestar(route_handlers=[sse_handler])



@get(["/testwebsocket"], sync_to_thread=False)
async def testwebsocket(request: Request,channels: ChannelsPlugin) -> str:
    [channels.publish(await getemoji(), channels=[chnl]) for chnl in request.app.dependencies['channels'].value._channels]
    



app = Litestar(
  route_handlers=[testwebsocket],
  plugins=[ChannelsPlugin( backend=MemoryChannelsBackend(history=10), arbitrary_channels_allowed=True,create_ws_route_handlers=True,ws_handler_send_history=7)],
)