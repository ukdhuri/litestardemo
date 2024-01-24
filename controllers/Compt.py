

from typing import Optional
from benedict import benedict
from litestar import Controller, Request, get, post
from sqlalchemy.ext.asyncio import AsyncSession
from litestar.response import Template
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation,TriggerEvent,ClientRedirect
from icecream import ic
from lib.service import get_tables_list,get_column_list
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
    

    @post(["/refresh_both"], sync_to_thread=False)
    async def refresh_both(
        self,
        data : TCompareModel,
        request: Request,
        transaction_remote: AsyncSession      ,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
    ) ->  Template:
        ic(data)
        benecontext = benedict()
        main_compare_types = {}

        for t_types in TComareTypes:
            main_compare_types[t_types.name] = t_types.value

        if data.left_db != 'Select Database' and data.left_db != '':
            tables = await get_tables_list(request,transaction_remote,data.left_db)
            benecontext['common.left.tables'] = tables
        else:
            benecontext['common.left.tables'] = []

        if data.right_db != 'Select Database' and data.right_db != '':
            tables = await get_tables_list(request,transaction_remote,data.right_db)
            benecontext['common.right.tables'] = tables
        else:
            benecontext['common.right.tables'] = []

        if data.left_tbl != 'Select Table' and data.left_tbl != '':
            columns_left = await get_column_list(request,transaction_remote,data.left_db,data.left_tbl)
            benecontext['common.left.columns'] = columns_left
        else:
            benecontext['common.left.columns'] = []

        if data.right_tbl != 'Select Table' and data.right_tbl != '':
            columns_right = await get_column_list(request,transaction_remote,data.right_db,data.right_tbl)
            benecontext['common.right.columns'] = columns_right
        else:
            benecontext['common.right.columns'] = []

        ic(CLIENT_SESSION_ID)
        benecontext['main_compare_types'] = main_compare_types
        benecontext['t_compare_objects'] = dict(data)
        benecontext['CLIENT_SESSION_ID'] = CLIENT_SESSION_ID
        benecontext['main_compare_types'] = main_compare_types
        benecontext['db_names'] = static.SConstants.db_names.keys()
        benecontext['common.left.db'] = data.left_db
        benecontext['common.right.db'] = data.right_db
        benecontext['common.left.tbl'] = data.left_tbl
        benecontext['common.right.tbl'] = data.right_tbl
        benecontext['common.left.pivot_choice'] = data.left_pivot_choice
        benecontext['common.right.pivot_choice'] = data.right_pivot_choice
        benecontext['common.left.pivot_column'] = data.left_pivot_column
        benecontext['common.right.pivot_column'] = data.right_pivot_column
        benecontext['date_formats'] = static.SConstants.date_formats
        benecontext['common.left.pivot_format'] = data.left_pivot_format
        benecontext['common.right.pivot_format'] = data.right_pivot_format
        return HTMXTemplate(
                template_name='fragments/comparator_frag.html',context=benecontext
        )