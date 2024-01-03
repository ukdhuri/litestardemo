from loguru import logger
from apscheduler.schedulers.asyncio   import AsyncIOScheduler
from lib.dependencies import provide_transaction_remote
from lib.service import get_product_fn,get_recent_orders
from sqlalchemy.ext.asyncio import  create_async_engine
async def scheduled_task(state):
    #session = await anext(provide_transaction_remote(state))
    engine = create_async_engine(state.remote_con_str)
    async with engine.connect() as conn:
        #orders = await get_recent_orders(conn)
        logger.debug(f"  👌👌👌👌")

async def start_scheduler(app):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(scheduled_task, "interval", seconds=9999, args=[app.state])
    scheduler.start()
    app.state['scheduler'] = scheduler
    #asyncio.get_event_loop().run_forever()

async def stop_scheduler(app):
    scheduler = app.state['scheduler']
    logger.debug(f"{len(scheduler.get_jobs())} {app.state['remote_con_str']}  👌👌👌👌")
    scheduler.remove_all_jobs()
    scheduler.shutdown()