

from typing import Optional
from benedict import benedict
from litestar import Controller, Request, Response, get, post
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from litestar.response import Template
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation,TriggerEvent,ClientRedirect
from lib.service import get_tables_list,get_column_list,process_compare_post
from icecream import ic
from models.TCompare import TComareTypes, TCompareModel
from typing import Annotated
from litestar.params import Parameter
import uuid
from datetime import timedelta
import pickle
import static.SConstants


class ComptController(Controller):
    path = "/compt"

    @get(["/"], sync_to_thread=False)
    async def compt(
        self,
        request: Request,
        transaction_remote: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
        force_refresh: Annotated[Optional[str], Parameter(query="force_refresh")],

    ) ->  Template:
        request.session['CLIENT_SESSION_ID']=""
        benecontext = benedict()
        if CLIENT_SESSION_ID is None:
            CLIENT_SESSION_ID = str(uuid.uuid4())
            benecontext['CLIENT_SESSION_ID'] = CLIENT_SESSION_ID
        main_compare_types = {}
        for t_types in TComareTypes:
            main_compare_types[t_types.name] = t_types.value
        tm = TCompareModel()
        benecontext['main_compare_types'] = main_compare_types
        benecontext['t_compare_objects'] = dict(tm)
        return HTMXTemplate(
                template_name='compt.html',context=benecontext  
        )
    

    @post(["/refresh_both"], sync_to_thread=False)
    async def refresh_both(
        self,
        data : dict,
        request: Request,
        transaction_remote: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
    ) ->  Template:

        benecontext = await process_compare_post(request=request, data=data, transaction_remote=transaction_remote, CLIENT_SESSION_ID=CLIENT_SESSION_ID)
        #ic(benecontext)
        return HTMXTemplate(
            template_name='fragments/comparator_frag.html',context=benecontext
        )
    

    @post(["/saveconfig"], sync_to_thread=False)
    async def saveconfig(
        self,
        data : dict,
        request: Request,
        transaction_remote: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
    ) ->  Template:
        benecontext = await process_compare_post(request=request, data=data, transaction_remote=transaction_remote, CLIENT_SESSION_ID=CLIENT_SESSION_ID,validate_model=True)
        ic(benecontext)
        return HTMXTemplate(
            template_name='fragments/comparator_frag.html',context=benecontext
        )
        
