

from typing import Optional
from benedict import benedict
from litestar import Controller, Request, get, post
from sqlalchemy.ext.asyncio import AsyncSession
from litestar.response import Template
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation,TriggerEvent,ClientRedirect
from icecream import ic
from lib.service import get_tables_list
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
    

    @post(["/refresh_left"], sync_to_thread=False)
    async def refresh_left(
        self,
        data : TCompareModel,
        request: Request,
        transaction_remote: AsyncSession      ,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
    ) ->  Template:
        benecontext = benedict()
        main_compare_types = {}

        for t_types in TComareTypes:
            main_compare_types[t_types.name] = t_types.value

        if data.selected_left_db != 'Select Table' and data.selected_left_db != '':
            tables = await get_tables_list(request,transaction_remote,data.selected_left_db)
            benecontext['common.left.tables'] = tables
        else:
            benecontext['common.left.tables'] = []
        ic(CLIENT_SESSION_ID)
        benecontext['main_compare_types'] = main_compare_types
        benecontext['t_compare_objects'] = dict(data)
        benecontext['CLIENT_SESSION_ID'] = CLIENT_SESSION_ID
        benecontext['main_compare_types'] = main_compare_types
        benecontext['db_names'] = static.SConstants.db_names.keys()
        benecontext['selected_left_db'] = data.selected_left_db
        benecontext['selected_right_db'] = data.selected_right_db
        benecontext['selected_left_tbl'] = data.selected_left_tbl
        benecontext['selected_right_tbl'] = data.selected_right_tbl

        return HTMXTemplate(
                template_name='fragments/comparator_frag.html',context=benecontext
        )