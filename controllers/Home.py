from typing import Optional, Union
from litestar import Controller, Request, Response, get
from sqlmodel import or_, select
from models import remote
from sqlalchemy.ext.asyncio import AsyncSession
from lib.util import get_todo_listX,get_all_users
from litestar.contrib.htmx.request import HTMXRequest
from litestar.contrib.htmx.response import HTMXTemplate
from litestar.response import Template
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
import asyncio


async def get_users_fn(transaction_remote: AsyncSession, search_query: str = "", page_number: int = 1, page_size: int = 12)-> list[remote.User]:
    query=select(remote.User)
    if search_query:
        query = query.filter(
            or_(
                remote.User.name.ilike(f"%{search_query}%"),
                remote.User.address.ilike(f"%{search_query}%"),
                remote.User.email.ilike(f"%{search_query}%"),
                # Add more columns as needed
            )
        )
    if page_size == -1:
        query = query.order_by(remote.User.id.asc())
        print(query)
    else:
        query = query.order_by(remote.User.id.asc()).offset((page_number - 1) * page_size).limit(page_size)
    
    result = await transaction_remote.execute(query)
    return result.scalars().all()


class HomeController(Controller):
    path = "/"

    @get(["/home"], sync_to_thread=False) 
    async def home(controller,transaction_remote: AsyncSession,request: HTMXRequest, search_query: Optional[str] , page_number: int = 1, page_size: int = 12) ->  Template:
        lpush_url="/home"
        userlist = await get_users_fn(transaction_remote,search_query, page_number, page_size)
        #await asyncio.sleep(2)
        if len(userlist) == 0:
            context = {"toastmessage": "No Records Found!!"}
            if request.htmx and search_query:
                lpush_url=f"/home/?search_query={search_query}"
            return HTMXTemplate(
                template_name="fragments/warning.html",
                context=context,
                re_swap="beforebegin",  # change swapping method
                re_target="#maincontent",  # change target element
                push_url=lpush_url
            )
            #return Response(status_code=204,content="")
        else:
            next_page_number = page_number + 1
            lpush_url=f"/home/?page_number={page_number}&page_size={page_size}"
            context = {"title": "Home","userlist": userlist,"next_page_number": next_page_number,"page_size": page_size,"shouldusepagination" : True}
            if request.htmx:
                template= "fragments/tablerow.html"
            else:
                 template= "index.html"
            if search_query:
                context["shouldusepagination"] = False
                lpush_url=f"/home/?search_query={search_query}"
            return HTMXTemplate(
                    template_name=template, context=context, push_url=lpush_url
            )
            # if page_number == 1 or not request.htmx:
            #     if search_query:
            #         template= "index.html"
            #     else:
            #         context["search_query"] = search_query
            #         context["shouldusepagination"] = False
            #         template= "fragments/tablerow.html"
            #     return HTMXTemplate(
            #         template_name=template, context=context, push_url=f"/home/?page_number={page_number}&page_size={page_size}"
            #     )
            # else:
            #     return HTMXTemplate(
            #             template_name="fragments/tablerow.html", context=context, 
            #     )
    
    @get(["/get_users"], sync_to_thread=False) 
    async def get_users(request: Request,transaction_remote: AsyncSession, page_number: int , page_size: int ) ->  list[remote.User]:
        return await get_users_fn(transaction_remote, page_number, page_size)
 