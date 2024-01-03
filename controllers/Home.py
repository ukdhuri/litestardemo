from typing import Optional, Union
from litestar import Controller, Request, Response, get
from sqlmodel import or_, select
from lib.service import get_recent_orders, get_users_fn, last_order_id
from models import remote
from sqlalchemy.ext.asyncio import AsyncSession
from lib.util import get_todo_listX,get_all_users
from litestar.contrib.htmx.request import HTMXRequest
from litestar.contrib.htmx.response import HTMXTemplate
from litestar.response import Template
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
import asyncio
from datetime import datetime,timedelta




class HomeController(Controller):
    path = "/"

    @get(["/home"], sync_to_thread=False) 
    async def home(controller,transaction_remote: AsyncSession,request: HTMXRequest, search_query: Optional[str] = "", page_number: int = 1, page_size: int = 50) ->  Template:
        lpush_url="/home"
        userlist = await get_users_fn(transaction_remote,search_query, page_number, page_size,scalar=True)
        print(userlist)
        #userlist = [{'id': 1, 'name': 'Alice', 'password': 25, "email": "a@a" ,"phone_number": "123456"}, {'id': 2, 'name': 'Bob', 'password': 30, "email": "a@a" ,"phone_number": "123456"}]
        #await asyncio.sleep(2)
        if len(userlist) == 0:
            context = {"toastmessage": "No Records Found!!","search_text": search_query}
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
            context = {"title": "Home","userlist": userlist,"next_page_number": next_page_number,"page_size": page_size,"shouldusepagination" : True,"search_text": search_query}
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


    @get(path="/preview")
    async def preview(self,request: HTMXRequest) -> Template:
        htmx = request.htmx  #  if true will return HTMXDetails class object
        if htmx:
            print(htmx.current_url)
        # OR
        if request.htmx:
            print(request.htmx.current_url)
        return HTMXTemplate(template_name="indexh.html", context="")
    

    @get(path="/preview2")
    async def preview2(self,request: HTMXRequest) -> Template:
        htmx = request.htmx  #  if true will return HTMXDetails class object
        if htmx:
            print(htmx.current_url)
        # OR
        if request.htmx:
            print(request.htmx.current_url)
        return HTMXTemplate(template_name="index2.html", context="")
    
    
    
    @get(["/get_users2"], sync_to_thread=False) 
    async def get_users2(request: Request,transaction_remote: AsyncSession, page_number: int , page_size: int ) ->  list[remote.User]:
        await get_users_fn(transaction_remote, page_number, page_size)
        await get_users_fn(transaction_remote, page_number, page_size)


    @get(["/liveorderlongpoll"], sync_to_thread=False, cache=50) 
    async def liveorderlongpoll(self,transaction_remote: AsyncSession,request: Request, search_query: str = "", page_number: int = 1, page_size: int = 33) -> list[object]:
        return await get_recent_orders(transaction_remote, search_query , page_number, page_size )
        # neworderid = await last_order_id(transaction_remote)
        # st =   request.app.stores.get("memory",)
        # lastorderid = await st.get("lastorderid",)
        # if lastorderid:
        #     if lastorderid!=neworderid:
        #         await st.set("lastorderid", str(neworderid))



       




