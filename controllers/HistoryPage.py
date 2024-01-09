from typing import Optional, Union
from litestar import Controller, Request, Response, get
from sqlmodel import or_, select
from lib.service import get_recent_orders, get_users_fn, last_order_id
from models import remote
from sqlalchemy.ext.asyncio import AsyncSession
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


pagealias = {
    "page": "history",
}


class datafunctions:
    def users():
        return "<h1>History Page</h1>"




class HistoryPageController(Controller):
    path = "/history"


    @get(["/"], sync_to_thread=False)
    async def get_history(
        controller,
        transaction_remote: AsyncSession,
        request: HTMXRequest,
        item_name: StopIteration = 1,
        search_query: Optional[str] = "",
        page_number: int = 1,
        page_size: int = 50,
        order_by_column_str = ""
    ) -> Template:
     
        
        userlist = await get_users_fn(
            transaction_remote, search_query, page_number, page_size, scalar=True
        )
              
        context = {
                "title": "Home",
                "userlist": userlist,
                "next_page_number": next_page_number,
                "page_size": page_size,
                "shouldusepagination": True,
                "search_text": search_query,
            }