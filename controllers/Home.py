
import io
from pathlib import Path
from random import random
import string
from typing import Optional, Union
from httpx import post
from litestar import Controller, MediaType, Request, Response, get
import pandas as pd
from sqlmodel import or_, select
from lib.service import get_recent_orders, get_users_fn, last_order_id,get_historical_result,build_context
from models import remote,History
from sqlalchemy.ext.asyncio import AsyncSession
from lib.util import get_todo_listX, get_all_users
from litestar.contrib.htmx.request import HTMXRequest
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation,TriggerEvent,ClientRedirect
from litestar.response import Template
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.template.config import TemplateConfig
import asyncio
from datetime import datetime, timedelta
from litestar.response import File
from litestar import post
from litestar import get
from litestar.response import Template
import static.SConstants 
class HomeController(Controller):
    path = "/"

    @get(["/home","/","/index"], sync_to_thread=False)
    async def home(
        controller,
        transaction_remote: AsyncSession,
        request: HTMXRequest,
        search_query: Optional[str] = "",
        page_number: int = 1,
        page_size: int = 50,
    ) -> Template:
        lpush_url = "/home"
        if request.url.path != lpush_url:
            return HXLocation(redirect_to=lpush_url)
        userlist = await get_users_fn(
            transaction_remote, search_query, page_number, page_size, scalar=True
        )
        # userlist = [{'id': 1, 'name': 'Alice', 'password': 25, "email": "a@a" ,"phone_number": "123456"}, {'id': 2, 'name': 'Bob', 'password': 30, "email": "a@a" ,"phone_number": "123456"}]
        # await asyncio.sleep(2)
        if len(userlist) == 0:
            context = {
                "toastmessage": "No Records Found!!",
                "search_text": search_query,
            }
            if request.htmx and search_query:
                lpush_url = f"/home/?search_query={search_query}"
            return HTMXTemplate(
                template_name="fragments/warning.html",
                context=context,
                re_swap="beforebegin",  # change swapping method
                re_target="#maincontent",  # change target element
                push_url=lpush_url,
            )
            # return Response(status_code=204,content="")
        else:
            next_page_number = page_number + 1
            lpush_url = f"/home/?page_number={page_number}&page_size={page_size}"
            context = {
                "title": "Home",
                "userlist": userlist,
                "next_page_number": next_page_number,
                "page_size": page_size,
                "shouldusepagination": True,
                "search_text": search_query,
            }

            if request.htmx:
                template = "fragments/tablerow.html"
            else:
                template = "index.html"

            if search_query:
                context["shouldusepagination"] = False
                lpush_url = f"/home/?search_query={search_query}"
                
            return HTMXTemplate(
                template_name=template, context=context, push_url=lpush_url
            )

    @get(["/get_users"], sync_to_thread=False)
    async def get_users(
        request: Request,
        transaction_remote: AsyncSession,
        page_number: int,
        page_size: int,
    ) -> list[remote.User]:
        return await get_users_fn(transaction_remote, page_number, page_size)
    

    @get(["/get_users3"], sync_to_thread=False)
    async def get_users3(
        self,
        request: Request,
        transaction_remote: AsyncSession      
    ) ->  Template:
        tracker=History.SearchAndOrder()
        tracker.page_number = 1
        lhist = History.UserHistory()
        tracker.order_list = lhist.order_list
        achemylist = await get_historical_result(request=request, asyncsession = transaction_remote, dilect = static.SConstants.Dilects.TSQL.name, tracker=tracker, histmodel=lhist)
        rsultlist = [History.UserHistory(**row._asdict()) for row in achemylist]
        context = await build_context(title="History",resultlist=rsultlist,hist=lhist,data=tracker)
        return HTMXTemplate(
                template_name='history.html', context=context
        )
    





    @post(["/get_users3"], sync_to_thread=False)
    async def get_users3_post(
        self,
        request: Request,
        transaction_remote: AsyncSession ,
        data: History.SearchAndOrder
    ) ->  Template:
        print(data)
        achemylist = await get_historical_result(request=request, asyncsession = transaction_remote,
                                                   scalar=False, tracker=data, histmodel=History.UserHistory)
        rsultlist = [History.UserHistory(**row._asdict()) for row in achemylist]      
        lhist = History.UserHistory()
        context = await build_context(title="Home",resultlist=rsultlist,hist=lhist,data=data,update_order_section = True)
        template_name='fragments/table_and_rows.html'
        if data.page_number > 1:
            template_name='fragments/table_rows.html'
            return HTMXTemplate(
                    template_name=template_name,context=context,
                     re_swap="beforebegin",  # change swapping method
                     re_target=f"#history_footer",  # change target element
            )
        return HTMXTemplate(
            template_name=template_name,context=context,trigger_event="resetpagenumber",after="receive"
        )       

    @post(["/history_download"], sync_to_thread=False)
    async def history_download(
        self,
        request: Request,
        transaction_remote: AsyncSession ,
        data: History.SearchAndOrder
    ) -> ClientRedirect:
        lhist = History.UserHistory()
        achemylist = await get_historical_result(request=request, asyncsession = transaction_remote,
                                                   scalar=False, tracker=data, histmodel=History.UserHistory)
        rsultlist = [History.UserHistory(**row._asdict()) for row in achemylist]      
        df = pd.DataFrame.from_records(map(dict, rsultlist)).iloc[:, 1:]
        now = datetime.now()
        file = f'output_{now.strftime("%Y-%m-%d_%H-%M-%S%f")}.xlsx'
        filepath=Path(Path(__file__).resolve().parent.parent, "static", file)
        df.to_excel(filepath, index=False)
        return ClientRedirect(
            redirect_to=f"/static/{file}",           
        )
        return File(
            content_disposition_type="attachment",
            path=filepath,
            filename=file,
        )   
        return HXLocation(
            redirect_to=f"/static/{file}",  
        )
        return HTMXTemplate(
            template_name=template_name,context=context,trigger_event="resetpagenumber",after="receive"
        )
    
    @get(path="/move_to_next_page")
    async def move_to_next_page(self, request: HTMXRequest) -> TriggerEvent:
        return TriggerEvent(
            content="",
            name="forwardpage",
            after="swap",  # possible values 'receive', 'settle', and 'swap'
        )

    @get(path="/preview")
    async def preview(self, request: HTMXRequest) -> Template:
        htmx = request.htmx  #  if true will return HTMXDetails class object
        if htmx:
            print(htmx.current_url)
        # OR
        if request.htmx:
            print(request.htmx.current_url)
        return HTMXTemplate(template_name="indexh.html", context="")

    @get(path="/preview2")
    async def preview2(self, request: HTMXRequest) -> Template:
        htmx = request.htmx  #  if true will return HTMXDetails class object
        if htmx:
            print(htmx.current_url)
        # OR
        if request.htmx:
            print(request.htmx.current_url)
        return HTMXTemplate(template_name="preview2.html", context="")

    @get(["/get_users2"], sync_to_thread=False)
    async def get_users2(
        request: Request,
        transaction_remote: AsyncSession,
        page_number: int,
        page_size: int,
    ) -> list[remote.User]:
        await get_users_fn(transaction_remote, page_number, page_size)
        await get_users_fn(transaction_remote, page_number, page_size)

    @get(["/liveorderlongpoll"], sync_to_thread=False, cache=15)
    async def liveorderlongpoll(
        self,
        transaction_remote: AsyncSession,
        request: Request,
        search_query: Optional[str] = "",
        page_number: int = 1,
        page_size: int = 33,
        poll : int = 3
    ) -> Template:
        lpush_url = "/liveorderlongpoll"
        if request.url.path != lpush_url:
            return HXLocation(redirect_to=lpush_url)
        next_page_number = page_number + 1
        itemlist = await get_recent_orders(
            transaction_remote,
            page_number=page_number,
            page_size=page_size,
            scalar=False,
        )
        if len(itemlist) == 0:
            context = {"toastmessage": "No Records Found!!"}
            if request.htmx:
                lpush_url = lpush_url
                return HTMXTemplate(
                    template_name="fragments/warning.html",
                    context=context,
                    re_swap="beforebegin",  # change swapping method
                    re_target="#maincontent",  # change target element
                    push_url=lpush_url,
                )
        itemlist = [remote.OrderResult(**row._asdict()) for row in itemlist]
        lpush_url = (
            f"{lpush_url}" #/?page_number={page_number}&page_size={page_size}"
        )
        context = {
            "title": "Home",
            "itemlist": itemlist,
            "next_page_number": next_page_number,
            "page_size": page_size,
            "shouldusepagination": True,
        }
        if poll == 1:
            template = "fragments/tableroworder.html"
        elif poll == 3:
            template = "order.html"
        else:
            if request.htmx:
                template = "fragments/tableroworder.html"
            else:
                template = "order.html"
        return HTMXTemplate(
                template_name=template, context=context, push_url=lpush_url
            )
        
        # context = {"title": "Home","userlist": resultist,"next_page_number": next_page_number,"page_size": page_size,"shouldusepagination" : True,"search_text": search_query}
        # neworderid = await last_order_id(transaction_remote)
        # st =   request.app.stores.get("memory",)
        # lastorderid = await st.get("lastorderid",)
        # if lastorderid:
        #     if lastorderid!=neworderid:
        #         await st.set("lastorderid", str(neworderid))
