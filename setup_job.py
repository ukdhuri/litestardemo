import asyncio
import json
from hydra import compose
from sqlmodel import  SQLModel, col
import pandas as pd
from lib.util import get_engine
from models.TCompare import Tcomparemodel,Tschduledcomparerunmodel
from icecream import ic
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession

# <!-- <tbody  id="scr_tbody"  hx-swap-oob="afterbegin"> -->
#   hx-swap="multi:{% for config_run_row in config_run_id_tbl %}#scrid_{{ config_run_row.id }}{% if not loop.last %},{% endif %}{% endfor %}:afterned"
async def my_async_function():
    # Your asynchronous code here
    await asyncio.sleep(1)
    cfg = compose(config_name="config")

    async_engine = create_async_engine(
        cfg.local.vendor.connection_string,
        echo=True,
        future=True
    )

    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all,tables=[Tcomparemodel.__table__,  Tschduledcomparerunmodel.__table__])
        #await conn.run_sync(SQLModel.metadata.drop_all,tables=[Tschduledcomparerunmodel.__table__])

    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all,tables=[Tcomparemodel.__table__,  Tschduledcomparerunmodel.__table__])
        #await conn.run_sync(SQLModel.metadata.create_all,tables=[Tschduledcomparerunmodel.__table__])
    exit()

    async_session = sessionmaker(
        bind=async_engine, class_=AsyncSession, expire_on_commit=False
    )

    a =  Tcomparemodel(config_name="config1", left_obj_type="File", right_obj_type="Table", left_db="left_db", right_db="right_db", left_tbl="left_tbl", right_tbl="right_tbl", left_pivot_choice="left_pivot_choice", right_pivot_choice="right_pivot_choice", left_pivot_column="left_pivot_column", right_pivot_column="right_pivot_column", left_pivot_format="left_pivot_format", right_pivot_format="right_pivot_format", left_where_clause="left_where_clause", right_where_clause="right_where_clause", common_columns_str='["a", "b", "c"]', columns_to_exclude_str='["d", "e", "f"]', unique_columns_str='["g", "h", "i"]')
    b =  Tcomparemodel(config_name="config2", left_obj_type="File", right_obj_type="Table", left_db="left_db", right_db="right_db", left_tbl="left_tbl", right_tbl="right_tbl", left_pivot_choice="left_pivot_choice", right_pivot_choice="right_pivot_choice", left_pivot_column="left_pivot_column", right_pivot_column="right_pivot_column", left_pivot_format="left_pivot_format", right_pivot_format="right_pivot_format", left_where_clause="left_where_clause", right_where_clause="right_where_clause", common_columns_str='["a", "b", "c"]', columns_to_exclude_str='["d", "e", "f"]', unique_columns_str='["g", "h", "i"]')
    
    async with async_session() as session:
        async with session.begin():
            session.add(a)  
            session.add(b)
            await session.commit()
   


    mmm : Tcomparemodel = None
    async with async_session() as session:
        async with session.begin():
            statement = select(Tcomparemodel).where(col(Tcomparemodel.config_name) == 'config1')
            results = await session.exec(statement)
            mmm = results.all()[0]
            jsonstr = mmm.model_dump_json()
            json_dict = json.loads(jsonstr)
            #ic(json_dict)
        
    list_fields = [k for k, v in Tcomparemodel.__annotations__.items()]

    data_obj= Tcomparemodel(**json_dict)
    data_obj.left_tbl = "vvvvvvvvvvvvv"
    data_obj.left_obj_type = "CTE"
    #ic(data_obj)

    async with async_session() as session:
        async with session.begin():
            db_object = await session.get(Tcomparemodel, {"id": 1})
            hero_data = data_obj.model_dump(exclude_unset=True)
            for key, value in hero_data.items():
                if key in list_fields:
                    setattr(db_object, key, value)
            session.add(db_object)
            await session.commit()
            # session.refresh(db_object)
            # session.commit()

    async with async_session() as session:
        async with session.begin():       
            statement = select(Tcomparemodel).where(col(Tcomparemodel.config_name) == 'config1')
            results = await session.exec(statement)
            for x in results:
                #ic(x)
            
def main():
    # Call the asynchronous function
    asyncio.run(my_async_function())

if __name__ == "__main__":
    main()


