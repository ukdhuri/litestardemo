

from typing import Optional
from litestar import Controller, Request, get
from models import remote
from sqlalchemy.ext.asyncio import AsyncSession
from lib.dependencies import provide_transaction_remote
from lib.util import get_todo_listX

async def list_fn() -> list:
    print('xxx+')

class StatusController(Controller):
    path = "/status"


   
    @get(["/", "/yyy"],sync_to_thread=False) 
    async def handler(self, transaction_remote: AsyncSession) -> list[remote.User]:
        print(type(transaction_remote))
        return await get_todo_listX(None, transaction_remote)
    

    @get("/resource", sync_to_thread=False)
    def retrieve_resource(request: Request) -> str:
        return request.url
    
    #@logger.catch(reraise=True)

    # @get(["/getuserstatus"],sync_to_thread=False)
    # async def get_list(transaction: AsyncSession, done: Optional[bool] = None) -> list[all.User]:
    #     #retu
    #     return await get_todo_list(done, transaction)

        



