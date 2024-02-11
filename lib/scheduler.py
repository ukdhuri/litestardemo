from faker import Faker
from loguru import logger
from apscheduler.schedulers.asyncio   import AsyncIOScheduler
from lib.dependencies import provide_transaction_remote
from lib.service import get_product_fn,get_recent_orders
from sqlalchemy.ext.asyncio import  create_async_engine
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend
from litestar import Litestar, WebSocket, websocket
from datetime import datetime,timedelta
from lib.util import getemoji
import httpx
from litestar.response import Template
from litestar import Request
from litestar import Controller, MediaType, Request, Response, get

# channels_plugin = ChannelsPlugin(
#     backend=MemoryChannelsBackend(history=10),  # set the amount of messages per channel
#     # to keep in the backend
#     arbitrary_channels_allowed=True,
#     create_ws_route_handlers=True,
#     ws_handler_send_history=10,  # send 10 entries of the history by default
# )



    
#https://stackoverflow.com/questions/77540472/html-elements-in-response-using-htmxs-websockets-extension-are-getting-hyphens
async def scheduled_task(app: Litestar,channels: ChannelsPlugin):
    #[channels.publish(await getemoji(), channels=[chnl]) for chnl in app.dependencies['channels'].value._channels]
    # try:
    #     if 'is' not in app.state:
    #         Template(template_str='',context={'emo':'xxx'}).to_asgi_response(app,app.state['schedulerrequest'])
    #         app.state['is'] = True
    # except:
    #     raise 
        
    emo = await getemoji()
    fk = Faker()
    context = {'emo':emo}
    a1 = fk.name()
    a2 = fk.phone_number()
    a3 = fk.email()
    contextt = {'a1':a1,'a2':a2,'a3':a3}
    tstr =    """<tr>
        <td>{{ a1 }}</td>
        <td>{{ a2 }}</td>
        <td>{{ a3 }}</td>
      </tr>"""
    fragment_response=Template(template_str='<ul id="xxxx" hx-swap-oob="beforebegin:#receivexx"><li class="step step-primary" hx-swap-oob="beforebegin:#receivexx">{{ emo }}</li></ul>',context=context).to_asgi_response(app,app.state['schedulerrequest'])
    fragment_responset=Template(template_str=f'<tbody id="contentsx" hx-swap-oob="afterbegin">{tstr}</tbody>',context=contextt).to_asgi_response(app,app.state['schedulerrequest'])

    response=fragment_response.body.decode()
    responset=fragment_responset.body.decode()
    logger.debug(fragment_response.body.decode())
    channels.publish(response, channels=['emoji'])
    channels.publish(responset, channels=['tblw'])
    channels

    # if str(app.dependencies['channels'].value) != '_EmptyEnum.EMPTY':    
    #     channels_plugin : ChannelsPlugin = app.dependencies['channels'].value
    #     channels = channels_plugin._channels
    #     [channels_plugin.publish(await getemoji(), channels=[chnl]) for chnl in channels]
    # fake = Faker()
    # str = f'{fake.emoji()}{fake.emoji()}{fake.emoji()}{fake.emoji()}'
    # channels_plugin._channels['foo'].publish({"message": str}, "foo")
    # channels_plugin._channels.publish({"message": getemoji()}, "foo")
    # channels_plugin._channels.publish({"message": getemoji()}, "foo")
    # channels_plugin._channels.publish({"message": getemoji()}, "foo")
    # channels_plugin._channels.publish({"message": getemoji()}, "foo")
    #session = await anext(provide_transaction_remote(state))
    # engine = create_async_engine(state.remote_con_str)    
    # async with engine.connect() as conn:
    #     #orders = await get_recent_orders(conn)
    #     logger.debug(f"  👌👌👌👌")

async def scheduled_puiblisher(app,scheduler):
    try:
        if 'publisher_started' in app.state:
            return
        r = httpx.get('http://127.0.0.1:9999/testwebsocket',timeout=2)
        if r.status_code != 200:
            scheduler.add_job(scheduled_puiblisher, 'date', run_date=datetime.now() + timedelta(seconds=3), args=[app,scheduler])
            logger.debug(f"  👌👌👌👌")
    except:
        scheduler.add_job(scheduled_puiblisher, 'date', run_date=datetime.now() + timedelta(seconds=3), args=[app,scheduler])
        logger.debug(f"  👌👌👌👌")




async def start_scheduler(app):
    scheduler = AsyncIOScheduler()
    #scheduler.add_job(scheduled_puiblisher, 'date', run_date=datetime.now() + timedelta(seconds=3), args=[app,scheduler])
    scheduler.start()
    app.state['scheduler'] = scheduler

async def stop_scheduler(app):
    return

    if 'scheduler' in app.state:
        scheduler = app.state['scheduler']
        logger.debug(f"{len(scheduler.get_jobs())} {app.state['remote_con_str']}  👌👌👌👌")
        scheduler.remove_all_jobs()
        if scheduler.running:
            scheduler.shutdown()