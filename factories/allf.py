import queue
import random
from faker import Faker
import pandas as pd
from polyfactory.factories.pydantic_factory import ModelFactory
import pytz
import faker_commerce
from sqlalchemy import Engine, func, text
from sqlmodel import Session, select
from lib.util import generate_dates, reset_queue
from models import local, remote
from datetime import datetime,date,time,timedelta

global q
q = queue.Queue()


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








def produce_factory_object(engine : Engine,st: date , ed: date):
    global q
    q=reset_queue(q)
    uf=UserFactory()
    users=uf.batch(size=50)
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
    products=pf.batch(size=50)
    product_category_links : list[remote.ProductCategoryLink] = []
    for rnx in range(500):
        pcl = remote.ProductCategoryLink()
        pcl.product = random.choice(products)
        pcl.category = random.choice(unique_categories)
        product_category_links.append(pcl)

    fake = Faker()
    batchlist : list[remote.Batch] = []
    for dt in generate_dates(date(2021, 1, 1), date(2022, 1, 24)):
        st = datetime.combine(dt, time(hour=7, minute=00))
        et = datetime.combine(dt, time(hour=17, minute=00))
        nwet = fake.date_time_between_dates(datetime_start = et, datetime_end = et + timedelta(seconds=random.randint(1000, 3600)))
        bd : remote.Batch = remote.Batch(batch_date=dt, status="completed", start_time=st, end_time= nwet)
        batchlist.append(bd)

    randombatch = random.choice(batchlist)

    orderlist = []
    for i in range(random.randint(6, 10)):
        order = remote.Order(user=random.choice(users),batch=randombatch)
        order_item_list = []
        for j in range(random.randint(1, 5)):
            qty = random.randint(1, 5)
            orderitem = remote.OrderItem(product=random.choice(products), quantity=5)
            order_item_list.append(orderitem)
            #orders.orderitems.append(order_items)
        order.orderitems = order_item_list
        orderlist.append(order)


    with Session(engine) as session:
        session.add_all(users)
        session.add_all(unique_categories)
        session.add_all(products)
        session.add_all(product_category_links)
        session.add_all(orderlist)
        session.commit()

    df = pd.read_sql('select min(id) as id, product_id,category_id from [productcategorylink] group by product_id,category_id having count(1) > 1', engine)
    pcll : list[remote.ProductCategoryLink] = df.apply(lambda row: remote.ProductCategoryLink(**row), axis=1).tolist()



    stmt = (
        select(remote.ProductCategoryLink.product_id, remote.ProductCategoryLink.category_id, func.count(), func.min(remote.ProductCategoryLink.id))
        .group_by(remote.ProductCategoryLink.product_id, remote.ProductCategoryLink.category_id)
        .having(func.count() > 1)
        .distinct()
    )
    stmt = (
        select(remote.ProductCategoryLink.product_id, remote.ProductCategoryLink.category_id, func.min(remote.ProductCategoryLink.id))
        .group_by(remote.ProductCategoryLink.product_id, remote.ProductCategoryLink.category_id)
        .having(func.count() > 1)
    )

    print(stmt)

    delete_stment = """
    WITH cte AS (
        SELECT product_id, category_id,
            ROW_NUMBER() OVER (PARTITION BY product_id, category_id ORDER BY id) AS rn
        FROM [dbo].[productcategorylink]
    )
    DELETE FROM cte WHERE rn > 1;
    """
    with Session(engine) as session:
        result = session.exec(stmt).all()
        [print(rec.product_id,rec.category_id) for rec in result]

    with Session(engine) as session:
        session.exec(text(delete_stment))
        session.commit()
        
    with Session(engine) as session:
        result = session.exec(stmt).all()
        [print(rec.product_id,rec.category_id) for rec in result]


#https://stackoverflow.com/questions/55836752/i-need-to-generate-1000-unique-first-name-in-python