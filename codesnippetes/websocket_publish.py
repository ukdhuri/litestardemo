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




# @asynccontextmanager
# async def chat_room_lifespan(socket: WebSocket, channels: ChannelsPlugin,chatroomname: str) -> AsyncContextManager[None]:
#   async with channels.start_subscription(chatroomname, history=10) as subscriber:
#     try:
#       async with subscriber.run_in_background(socket.send_data):
#         yield
#     except WebSocketDisconnect:
#         return
# @websocket_listener("/ws/{chatroomname:str}", connection_lifespan=chat_room_lifespan)
# def chat_handler(chatroomname: str, data: str, channels: ChannelsPlugin) -> None:
#   channels.publish(data, channels=[chatroomname])
# app = Litestar(
#   route_handlers=[testwebsocket,chat_handler],
#   plugins=[ChannelsPlugin( backend=MemoryChannelsBackend(history=10), arbitrary_channels_allowed=True,ws_handler_send_history=7)],
# )



@get(["/testwebsocket"], sync_to_thread=False)
async def testwebsocket(request: Request,channels: ChannelsPlugin) -> str:
    [channels.publish(await getemoji(), channels=[chnl]) for chnl in request.app.dependencies['channels'].value._channels]
    

app = Litestar(
  route_handlers=[testwebsocket],
  plugins=[ChannelsPlugin(channels=["emoji"], backend=MemoryChannelsBackend(history=10), arbitrary_channels_allowed=True,create_ws_route_handlers=True,ws_handler_send_history=7)],
)





# @asynccontextmanager
# async def chat_room_lifespan(socket: WebSocket, channels: ChannelsPlugin) -> AsyncContextManager[None]:
#   async with channels.start_subscription("chat", history=10) as subscriber:
#     try:
#       async with subscriber.run_in_background(socket.send_data):
#         yield
#     except WebSocketDisconnect:
#         return


# @websocket_listener("/ws", connection_lifespan=chat_room_lifespan)
# def chat_handler(data: str, channels: ChannelsPlugin) -> None:
#   channels.publish(data, channels=["chat"])


# app = Litestar(
#   route_handlers=[chat_handler],
#   plugins=[ChannelsPlugin(channels=["chat"], backend=MemoryChannelsBackend(history=10))],
#   static_files_config=[StaticFilesConfig(directories=["static"], path="/", html_mode=True)],
# )