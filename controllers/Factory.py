import queue
from typing import Optional, Union
from litestar import Controller, Request, Response, get, post
from loguru import logger
from sqlmodel import or_, select
from lib.service import get_batch_fn,get_users_fn,upsert_record,upsert_records,get_product_fn,get_order_fn
from models import local, remote
from sqlalchemy.ext.asyncio  import AsyncSession
from litestar.contrib.htmx.request import HTMXRequest
from litestar.contrib.htmx.response import HTMXTemplate
from litestar.response import Template
import random
from faker import Faker
import pandas as pd
from polyfactory.factories.pydantic_factory import ModelFactory
import pytz
import faker_commerce
from sqlalchemy import Engine, MetaData, func, text
from lib.util import generate_dates, reset_queue
from datetime import datetime,date,time,timedelta

global q
q = queue.Queue()
products = []
unique_categories = []

class DenormalizedOrderFactory(ModelFactory[local.DenormalizedOrder]):
    __model__ = local.DenormalizedOrder

class UserFactory(ModelFactory[remote.User]):
    __faker__ = Faker()
    __fake_float_seed__ : int
    _queue_ = None


    @classmethod
    def id(cls) -> str:
        cls.__fake_float_seed__= q.get()
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__fake_float_seed__

    @classmethod
    def name(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.name()

    @classmethod
    def email(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.email()

    @classmethod
    def surname(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.name()

    @classmethod
    def phone_number(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.phone_number()

    @classmethod
    def address(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.address()


    @classmethod
    def date_of_birth(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.date_of_birth(tzinfo=pytz.timezone('America/Toronto'), minimum_age=18, maximum_age=65)



class CategoryFactory(ModelFactory[remote.Category]):
    __faker__ = Faker()
    __faker__.add_provider(faker_commerce.Provider)
    __fake_float_seed__ : int
    _queue_ = None

    @classmethod
    def id(cls) -> str:
        global q
        cls.__fake_float_seed__= q.get()
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__fake_float_seed__

    @classmethod
    def name(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.ecommerce_category()

class ProductFactory(ModelFactory[remote.Product]):
    __faker__ = Faker()
    __faker__.add_provider(faker_commerce.Provider)
    __fake_float_seed__ : int
    _queue_ = None

    @classmethod
    def id(cls) -> str:
        cls.__fake_float_seed__= q.get()
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__fake_float_seed__

    @classmethod
    def name(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__faker__.ecommerce_name()
    
    @classmethod
    def description(cls) -> str:
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        __random_seed__ = cls.__fake_float_seed__
        return cls.__faker__.paragraph(nb_sentences=2)

    
class ProductCategoryLinkFactory(ModelFactory[remote.ProductCategoryLink]):
    __faker__ = Faker()
    __faker__.add_provider(faker_commerce.Provider)
    __fake_float_seed__ : int
    _queue_ = None

    @classmethod
    def id(cls) -> str:
        cls.__fake_float_seed__= q.get()
        cls.__faker__.seed_instance(cls.__fake_float_seed__)
        return cls.__fake_float_seed__
    
    @classmethod
    def category_id(cls) -> str:
        #cls.__faker__.seed_instance(cls.__fake_float_seed__)
        #__random_seed__ = cls.__fake_float_seed__
        return cls.__random__.choice(list(unique_categories)).id


    @classmethod
    def product_id(cls) -> str:
        #cls.__faker__.seed_instance(cls.__fake_float_seed__)
        #__random_seed__ = cls.__fake_float_seed__
        return cls.__random__.choice(list(products)).id







class FactoryController(Controller):
    path = "/factory"

    @get(["/clean"], sync_to_thread=False)
    async def clean(self,request: Request,transaction_remote: AsyncSession) ->  str:
        stmt = (
        select(remote.ProductCategoryLink.product_id, remote.ProductCategoryLink.category_id, func.min(remote.ProductCategoryLink.id))
        .group_by(remote.ProductCategoryLink.product_id, remote.ProductCategoryLink.category_id)
        .having(func.count() > 1)
        )

        print(stmt)



        # result = await transaction_remote.execute(stmt)
        # logger.info(result.all())
        
    
        delete_stment = """
        WITH cte AS (
            SELECT product_id, category_id,
                ROW_NUMBER() OVER (PARTITION BY product_id, category_id ORDER BY id) AS rn
            FROM [dbo].[productcategorylink]
        )
        DELETE FROM cte WHERE rn > 1;
        """


        result = await transaction_remote.execute(stmt)
        [print(rec.product_id,rec.category_id) for rec in result.all()]

   
        await transaction_remote.execute(text(delete_stment))
        #transaction_remote.commit()
            
        result = await transaction_remote.execute(stmt)
        [print(rec.product_id,rec.category_id) for rec in result.all()] 

        return "cleaned"  


    @get(["/"], sync_to_thread=False) 
    async def build(self,request: Request,transaction_remote: AsyncSession,st: date , et: date) ->  str:
        logger.info("Factory Build Controller")
        user_btch = 240
        product_btch = 2400
        order_btch = 10000
        pcl_btch = 6000

        global q
        q=reset_queue(q)
        uf=UserFactory()
        users=uf.batch(size=user_btch)
        categories=CategoryFactory.batch(size=50)
        unique_categories = list({category.name: category for category in categories}.values())
        categories = unique_categories
        i = 0
        for categ in unique_categories:
            categ.id = i+1
            i = i+1
        q=reset_queue(q)
        pf=ProductFactory()
        pf._queue_=q
        products=pf.batch(size=product_btch)
        product_category_links : list[remote.ProductCategoryLink] = []
        for _ in range(pcl_btch):
            pcl = remote.ProductCategoryLink()
            pcl.product = random.choice(products)
            pcl.category = random.choice(unique_categories)
            product_category_links.append(pcl)

        fake = Faker()
        batchlist : list[remote.Batch] = []
        for dt in generate_dates(st, et):
            st = datetime.combine(dt, time(hour=7, minute=00))
            et = datetime.combine(dt, time(hour=17, minute=00))
            nwet = fake.date_time_between_dates(datetime_start = et, datetime_end = et + timedelta(seconds=random.randint(1000, 3600)))
            bd : remote.Batch = remote.Batch(batch_date=dt, status="completed", start_time=st, end_time= nwet)
            batchlist.append(bd)


        orderlist = []
        #for i in range(random.randint(6, 10)):
        for i in range(order_btch):   
            randomebatch = random.choice(batchlist)
            start_time= randomebatch.start_time + timedelta(hours=random.randint(1, 6))
            end_time = start_time + timedelta(seconds=random.randint(50, 250))
            order = remote.Order(user=random.choice(users),batch=randomebatch,start_time=start_time,end_time=end_time)
            order_item_list = []
            for j in range(random.randint(1, 6)):
                qty = random.randint(1,6)
                orderitem = remote.OrderItem(product=random.choice(products), quantity=qty)
                order_item_list.append(orderitem)
            order.orderitems = order_item_list
            orderlist.append(order)

        transaction_remote.add_all(users)
        transaction_remote.add_all(unique_categories)
        transaction_remote.add_all(products)
        transaction_remote.add_all(product_category_links)
        transaction_remote.add_all(orderlist)
        await transaction_remote.commit()
        

    
    @get(["/startbatch"], sync_to_thread=False) 
    async def startbatch(self,request: Request,transaction_remote: AsyncSession,st: date) -> remote.Batch:
        query = select(remote.Batch).where(remote.Batch.batch_date == st)
        #query = select(remote.User)
        result = await transaction_remote.execute(query)
        batch = result.scalars().first()
        print(st)
        print(batch)
        batch.status = "inprogress"
        transaction_remote.add(batch)
        await transaction_remote.commit()
        return batch



    @get(["/addrandomorder/{num:int}"], sync_to_thread=False) 
    async def addrandomorder(self,request: Request,transaction_remote: AsyncSession,num: int) ->  list[remote.Order]:
        users = await get_users_fn(transaction_remote,page_size=-1,scalar=True)
        batchlist=[]
        batchlist.append(await get_batch_fn(transaction_remote, status="inprogress"))
        orderlist = []
        #for i in range(random.randint(6, 10)):
        for i in range(num):   
            randomebatch = random.choice(batchlist)
            start_time= randomebatch.start_time + timedelta(hours=random.randint(1, 6))
            end_time = start_time + timedelta(seconds=random.randint(50, 250))
            user=random.choice(users)
            #user=remote.OrderResult(**user._asdict())
            batch=randomebatch
            order = remote.Order(user=user,batch=batch,start_time=start_time,end_time=end_time,orderitems=[])
            # order_item_list = []
            # for j in range(random.randint(1, 6)):
            #     qty = random.randint(1,6)
            #     orderitem = remote.OrderItem(product=random.choice(products), quantity=qty)
            #     order_item_list.append(orderitem)
            # order.orderitems = order_item_list
            orderlist.append(order)
        await upsert_records(transaction_remote,orderlist)
        # for order in orderlist:
        #     order.orderitems = []
        logger.info(orderlist)
        return orderlist

    @get(["/addrandomorderitems/{num:int}"], sync_to_thread=False) 
    async def addrandomorderitems(self,request: Request,transaction_remote: AsyncSession,num: int) ->  list[remote.OrderItem]:
        orders = await get_order_fn(transaction_remote,page_size=-1)
        batchlist=[]
        batchlist.append(await get_batch_fn(transaction_remote, status="inprogress"))
        orderlist = []
        products = await get_product_fn(transaction_remote,page_size=-1)
        order_item_list = []   
        for i in range(num):
            
            randomebatch = batchlist[0]
            orderitem = remote.OrderItem()
            # orderitem.order = random.choice(orders)
            # orderitem.product = random.choice(products)
            orderitem.order_id = random.choice(orders).id
            orderitem.product_id = random.choice(products).id
            orderitem.quantity = random.randint(1,6)
            order_item_list.append(orderitem)
        await upsert_records(transaction_remote,order_item_list)
        logger.info(order_item_list)
        return order_item_list



    @get(["/close"], sync_to_thread=False) 
    async def liveb(self,request: Request,transaction_remote: AsyncSession,st: date) ->  str:
        query = select(remote.Batch).where(remote.Batch.batch_date == st)
        result = await transaction_remote.execute(query)
        batch : remote.Batch = result.scalars().first()
        batch.status = "completed"
        transaction_remote.add(batch)
        await transaction_remote.commit()   

    @get(["/liveb"], sync_to_thread=False) 
    async def liveb(self,request: Request,transaction_remote: AsyncSession) -> remote.Batch:
        query = select(remote.Batch).where(remote.Batch.status == 'inprogress')
        #query = select(remote.User)
        result = await transaction_remote.execute(query)
        batch = result.scalars().first()
        print(batch)
        return batch

    @get(["/teststores"], sync_to_thread=False) 
    async def teststores(self,request: Request) -> str:
        st =   request.app.stores.get("tempdatafs")
        await st.set("foo", b"vvvv", expires_in=timedelta(minutes=15))
        x = str(type(st))
        return x
    
    
    @get(["/teststorep"], sync_to_thread=False) 
    @logger.catch(reraise=True)
    async def teststorep(self,request: Request) -> str:
        st =   request.app.stores.get("tempdatafs")
        rv = await st.get("foo")
        return rv