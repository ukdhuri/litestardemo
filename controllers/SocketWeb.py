from typing import Optional, Union
from litestar import Controller, MediaType, Request, Response, get
from sqlmodel import or_, select
from lib.service import get_recent_orders, get_users_fn, last_order_id
from models import remote
#from sqlalchemy.ext.asyncio  import AsyncSession
from sqlmodel.ext.asyncio.session import AsyncSession
from lib.util import get_todo_listX, get_all_users
from litestar.contrib.htmx.request import HTMXRequest
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation
from litestar.response import Template
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
import asyncio
from datetime import datetime, timedelta
from litestar.response import File
from pathlib import Path
from litestar import get

class SocketWebController(Controller):
    path = "/socketweb"


    @get(["/"], sync_to_thread=False) 
    async def build(self, request: Request) ->  Template:
        context = {'title':'ssx test'}
        #strx=Template(template_str="Hello <strong>{{ title }}</strong> using strings",context=context).to_asgi_response(request.app, request).body
        return HTMXTemplate(
                template_name='socketweb.html', context=context
            )
        
    


# from litestar import Controller, get
# from litestar.response import Template
# from litestar.contrib.htmx.response import HTMXTemplate, HXLocation

# class SocketWebController(Controller):
#     path = "/socketweb"
#     @get(["/"], sync_to_thread=False)
#     def wbd() -> Template:
#         context = {'title':'sse test'}
#         return HTMXTemplate(
#             template_name='sset.html', context=context, push_url='/sset'
#         )
# #<li data-content="★" class="step step-primary">Choose plan</li>
