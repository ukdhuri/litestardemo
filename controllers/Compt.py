

from pathlib import Path
from typing import Optional
from benedict import benedict
from litestar import Controller, Request, Response, get, post
from pydantic import ValidationError
from sqlmodel import col, select
#from sqlalchemy.ext.asyncio  import AsyncSession
from sqlmodel.ext.asyncio.session import AsyncSession
from litestar.response import Template
from litestar.contrib.htmx.response import HTMXTemplate, HXLocation,TriggerEvent,ClientRedirect
from lib.service import get_tables_list,get_column_list,process_compare_post, refresh_status_run_tbl, schedule_compare_run
from icecream import ic
from models.TCompare import TComareTypes, Tcomparemodel, Tschduledcomparerunmodel, get_configs, get_configs_by_conifg_name
from typing import Annotated
from litestar.params import Parameter
import uuid
from datetime import timedelta
import pickle
import static.SConstants
from litestar.background_tasks import BackgroundTask
from litestar.channels import ChannelsPlugin

class ComptController(Controller):
    path = "/compt"

    @get(["/configure_compare_limit"], sync_to_thread=False)
    async def configure_compare_limit(
        self,
        request: Request,
        conpare_limt: Annotated[Optional[int], Parameter(query="conpare_limt")],
    ) ->  str:
        trd = request.app.stores.get("memory")
        await trd.set(f"compare_limit",conpare_limt)
        return f"Success - compare_limit set to {conpare_limt}"



    @get(["/"], sync_to_thread=False)
    async def compt(
        self,
        request: Request,
        transaction_local: AsyncSession,
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
        tm = Tcomparemodel()
        configs = [ row for row in await get_configs(transaction_local)]
        benecontext['main_compare_types'] = main_compare_types
        benecontext['t_compare_objects'] = dict(tm)
        benecontext['configs'] = configs
        return HTMXTemplate(
                template_name='compt.html',context=benecontext  
        )
    

    @post(["/refresh_both"], sync_to_thread=False)
    async def refresh_both(
        self,
        data : dict,
        request: Request,
        transaction_remote: AsyncSession,
        transaction_remote1: AsyncSession,
        transaction_local: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
    ) ->  Template:
        benecontext = await process_compare_post(request=request, data=data, transaction_remote=transaction_remote,transaction_remote1=transaction_remote1, transaction_local=transaction_local, CLIENT_SESSION_ID=CLIENT_SESSION_ID)
        return HTMXTemplate(
            template_name='fragments/comparator_frag.html',context=benecontext
        )
    

    @post(["/saveconfig"], sync_to_thread=False)
    async def saveconfig(
        self,
        data : dict,
        request: Request,
        transaction_remote: AsyncSession,
        transaction_remote1: AsyncSession,
        transaction_local: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
    ) ->  Template:
        configs = [ row for row in await get_configs(transaction_local)]
        benecontext = await process_compare_post(request=request, data=data, transaction_remote=transaction_remote,transaction_remote1=transaction_remote1, transaction_local=transaction_local, CLIENT_SESSION_ID=CLIENT_SESSION_ID,save_model=True)
        if benecontext['id'] is not None:
            if benecontext['id'] != "":
                if len(str(benecontext['id'])) > 0:
                    try:
                        int(benecontext['id'])
                        configs.append(benecontext['config_name'])
                        benecontext['configs'] = configs
                    except ValueError:
                        pass
            return HTMXTemplate(
            template_name='fragments/comparator_frag.html',context=benecontext
        )

        
    @post(["/load_config"], sync_to_thread=False)
    async def load_config(
        self,
        data : dict,
        request: Request,
        transaction_remote: AsyncSession,
        transaction_remote1: AsyncSession,
        transaction_local: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
        channels: ChannelsPlugin
    ) ->  Template:
        configlist = await get_configs_by_conifg_name(transaction_local=transaction_local,config_name=data.get('config_name'))
        configs = [ row for row in await get_configs(transaction_local)]
        if len(configlist) == 0:
            config_to_Load = data
        else:
            config_to_Load = configlist[0].model_dump()
        #rsultlist = [Tcomparemodel(**row._asdict()) for row in configlist]
        benecontext = await process_compare_post(request=request, data=config_to_Load, transaction_remote=transaction_remote,transaction_remote1=transaction_remote1, transaction_local=transaction_local, CLIENT_SESSION_ID=CLIENT_SESSION_ID,save_model=False)
        #ic(benecontext)
        benecontext['config_name'] = data.get('config_name')
        if len(configlist) == 0:
            benecontext['id'] = ""
            benecontext['configs'] = configs
            return HTMXTemplate(
                template_name='fragments/comparator_frag.html',context=benecontext
            )
        else:
            benecontext['configs'] = configs
            return HTMXTemplate(
                template_name='fragments/comparator_frag.html',context=benecontext, background=BackgroundTask(refresh_status_run_tbl,request=request, asyncsession=transaction_local, config_id=benecontext['id'],channels=channels)
            )
    
    @post(["/schedule_compare_run"], sync_to_thread=False)
    async def schedule_compare_run(
        self,
        data : dict,
        request: Request,
        transaction_remote: AsyncSession,
        transaction_remote1: AsyncSession,
        transaction_local: AsyncSession,
        CLIENT_SESSION_ID: Annotated[Optional[str], Parameter(header="CLIENT_SESSION_ID")],
        channels: ChannelsPlugin
    ) ->  Template:
        #benecontext = await process_compare_post(request=request, data=data, transaction_remote=transaction_remote,transaction_remote1=transaction_remote1, transaction_local=transaction_local, CLIENT_SESSION_ID=CLIENT_SESSION_ID,save_model=True)
        bdata = benedict(data)
        #benecontext = await process_compare_post(request=request, data=data, transaction_remote=transaction_remote,transaction_remote1=transaction_remote1, transaction_local=transaction_local, CLIENT_SESSION_ID=CLIENT_SESSION_ID,save_model=False)
        return TriggerEvent(
            content="Success!",
            name="testLog",
            params={"attr": "value"},
            after="receive",  # possible values 'receive', 'settle', and 'swap'
            background=BackgroundTask(schedule_compare_run,request=request, transaction_remote=transaction_remote,transaction_remote1=transaction_remote1, transaction_local=transaction_local, config_id=bdata.id,channels=channels)

        )
        return HTMXTemplate(
            template_name='fragments/comparator_frag.html',context=benecontext,
            background=BackgroundTask(schedule_compare_run,request=request, asyncsession=transaction_local, config_id=bdata.id,channels=channels)
        )
    
    @get(["/csr_report_download"], sync_to_thread=False)
    async def history_download(
        self,
        request: Request,
        transaction_local: AsyncSession ,
        run_id: Annotated[Optional[int], Parameter(query="run_id")],
    ) -> ClientRedirect:
        statement = select(Tschduledcomparerunmodel).where(
            col(Tschduledcomparerunmodel.id) == run_id,
        )
        run_record_cur = await transaction_local.exec(statement)
        run_record = run_record_cur.one()
        #transaction_local.get(Tcomparemodel.r)
        return ClientRedirect(
            redirect_to=f"/reports/{run_record.run_report}",           
        )