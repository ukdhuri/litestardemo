import json
import pickle
from typing import Optional
from benedict import benedict
from litestar import Request
from loguru import logger
import pandas as pd
from sqlalchemy import text
from sqlmodel import SQLModel, or_, select
from lib.util import T, json_to_list, list_to_string,list_to_json,check_three_vars
from models import remote
#sqlalchemy.ext.asyncio
# from sqlalchemy.ext.asyxxxxxxxxncio  import AsyncSexxxxxxxxxssion
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, and_,Session
from datetime import date
from sqlmodel import  create_engine
import copy
from typing import TypeVar
from models.History import BaseHistory, SearchAndOrder
from models.TCompare import TComareTypes, TCompareModel
import static.SConstants
from litestar.response import Template
import static.SConstants
from datetime import timedelta
import pendulum
from datetime import date
from icecream import ic
from bokeh.plotting import figure, show
from bokeh.palettes import Category20c
from bokeh.transform import cumsum
from bokeh.io import output_notebook
from bokeh.models import ColumnDataSource
from bokeh.embed import components
from bokeh.io import curdoc
from bs4 import BeautifulSoup

def read_sql_query(con, stmt):
    return pd.read_sql(stmt, con)


async def get_df(stmt, engine):
    data = pd.read_sql(stmt, engine)
    return data


async def build_context(
    title: str,
    resultlist: list[BaseHistory],
    hist: BaseHistory,
    data: SearchAndOrder,
    update_order_section=False,
    shouldusepagination=False,
) -> dict:
    return {
        "title": title,
        "result_list": resultlist,
        "user_page_squeunce": hist.get_user_page_squeunce(),
        "sql_column_sequnce": hist.get_sql_column_sequnce(),
        "sql_order_sequnce": data.order_sequnce,
        "sql_order_direction": data.order_direction,
        "clm_name_mapping": static.SConstants.clm_name_mapping,
        "rev_direction": static.SConstants.rev_direction,
        "update_order_section": update_order_section,
    }


async def get_historical_result(
    request: Request,
    asyncsession: AsyncSession,
    scalar: bool = False,
    dilect: str = static.SConstants.Dilects.TSQL.name,
    tracker: Optional[SearchAndOrder] = None,
    histmodel: Optional[BaseHistory] = None,
):
    context = {
        "cte_select_statment": histmodel.get_select_clause(),
        "sql_column_sequnce": histmodel.get_sql_column_sequnce(),
        "sql_order_sequnce": tracker.order_sequnce,
        "sql_order_direction": tracker.order_direction,
        "direction_dict": static.SConstants.direction,
        "valid_search_columns": histmodel.get_sql_valid_search_columns(),
        "search_query": tracker.search_query,
        "page_number": tracker.page_number,
        "page_size": tracker.page_size,
        "dilect": dilect,
    }
    sql_statement = (
        Template(template_name="sql/history.txt", context=context)
        .to_asgi_response(request.app, request)
        .body.decode("utf-8")
    )
    sql_statement = str(sql_statement)
    print(sql_statement)
    result = await asyncsession.execute(text(str(sql_statement)))

    if scalar:
        res = result.scalars().all()
    else:
        res = result.all()
    return res
    # return [remote.OrderResult(**row._asdict()) for row in rows]


async def get_users_fn(
    transaction_remote: AsyncSession,
    search_query: str = "",
    page_number: int = 1,
    page_size: int = 33,
    scalar=False,
    order_by_column: str = "id",
    order_direction: str = "asc",
) -> list[remote.User]:
    query = select(remote.User)
    if search_query:
        query = query.filter(
            or_(
                remote.User.name.ilike(f"%{search_query}%"),
                remote.User.address.ilike(f"%{search_query}%"),
                remote.User.email.ilike(f"%{search_query}%"),
                # Add more columns as needed
            )
        )
    if page_size == -1:
        query = query.order_by(
            getattr(remote.User, order_by_column).asc()
            if order_direction == "asc"
            else getattr(remote.User, order_by_column).desc()
        )
    else:
        query = (
            query.order_by(
                getattr(remote.User, order_by_column).asc()
                if order_direction == "asc"
                else getattr(remote.User, order_by_column).desc()
            )
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )

    result = await transaction_remote.execute(query)
    if scalar:
        return result.scalars().all()
    else:
        return result.all()


async def get_recent_orders(
    transaction_remote: AsyncSession,
    search_query: str = "",
    page_number: int = 1,
    page_size: int = 33,
    scalar=False,
    order_by_column: str = "id",
    order_direction: str = "desc",
) -> list[remote.OrderResult]:
    batch = await get_batch_fn(transaction_remote, status="inprogress")
    ##different column scalr for html will not work
    query = (
        select(
            remote.Order.id,
            remote.Order.start_time,
            remote.Order.end_time,
            remote.Batch.batch_date,
            remote.User.name,
        )
        .join(remote.Batch, remote.Order.batch_id == remote.Batch.id)
        .join(remote.User, remote.Order.user_id == remote.User.id)
        .where(remote.Order.batch_id == batch.id)
        .where(
            or_(
                remote.User.name.contains(search_query),
                remote.User.address.contains(search_query),
            )
        )
    )
    if page_size == -1:
        query = query.order_by(
            getattr(remote.Order, order_by_column).asc()
            if order_direction == "asc"
            else getattr(remote.Order, order_by_column).desc()
        )
    else:
        query = (
            query.order_by(
                getattr(remote.Order, order_by_column).asc()
                if order_direction == "asc"
                else getattr(remote.Order, order_by_column).desc()
            )
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )
    result = await transaction_remote.execute(query)
    if scalar:
        res = result.scalars().all()
        return res
    else:
        res = result.all()
        return res
    # return [remote.OrderResult(**row._asdict()) for row in rows]


async def last_order_id(transaction_remote: AsyncSession) -> int:
    batch = await get_batch_fn(transaction_remote, status="inprogress")
    query = (
        select(
            remote.Order.id,
            remote.Order.start_time,
            remote.Order.end_time,
            remote.Batch.batch_date,
            remote.User.name,
            remote.User.address,
        )
        .join(remote.Batch, remote.Order.batch_id == remote.Batch.id)
        .where(
            remote.Order.batch_id == batch.id
            if batch.id
            else remote.Batch.batch_date == batch.batch_date
        )
        .order_by(remote.Order.id.desc())
    )
    query = query.order_by(remote.User.id.asc()).limit(1)
    result = await transaction_remote.execute(query)
    orderid = result.scalar().first()
    return orderid


async def get_batch_fn(
    transaction_remote: AsyncSession,
    batch_id: Optional[int] = None,
    batch_date: Optional[date] = None,
    status: Optional[str] = None,
) -> remote.Batch:
    query = select(remote.Batch).where(
        and_(
            remote.Batch.id == batch_id if batch_id else True,
            remote.Batch.batch_date == batch_date if batch_date else True,
            remote.Batch.status == status if status else True,
        )
    )
    print(query)
    result = await transaction_remote.execute(query)
    return result.one()[0]


async def get_product_fn(
    transaction_remote: AsyncSession,
    search_query: str = "",
    page_number: int = 1,
    page_size: int = 12,
) -> list[remote.Product]:
    query = select(remote.Product)
    if search_query:
        query = query.filter(
            or_(
                remote.Product.name.ilike(f"%{search_query}%"),
                remote.Product.description.ilike(f"%{search_query}%"),
                # Add more columns as needed
            )
        )
    if page_size == -1:
        query = query.order_by(remote.Product.id.asc())
        print(query)
    else:
        query = (
            query.order_by(remote.Product.id.asc())
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )

    result = await transaction_remote.execute(query)
    return result.all()


async def get_order_fn(
    transaction_remote: AsyncSession,
    batch_id: Optional[int] = None,
    page_number: int = 1,
    page_size: int = 12,
) -> list[remote.Order]:
    query = select(remote.Order).where(
        and_(
            remote.Order.id == batch_id if batch_id else True,
        )
    )
    if page_size == -1:
        query = query.order_by(remote.Order.id.asc())
        print(query)
    else:
        query = (
            query.order_by(remote.Order.id.asc())
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )

    result = await transaction_remote.execute(query)
    return result.all()

async def plot_pie_chart(inputdataset,idkey="process_id", statuskey='process_status'):
    dataset = pd.DataFrame.from_records(map(dict, inputdataset)).iloc[:, 1:]
    # Create a dictionary to map the status to a color
    colors = {'Completed': 'green', 'Failed': 'red', 'In Progress': 'orange'}
    # Group the dataset by status
    data = dataset.groupby(statuskey).count().reset_index()
    # Add a column for the angle of each wedge
    data['angle'] = data[idkey]/data[idkey].sum() * 2*3.14
    # Add a column for the color of each wedge
    data['color'] = [colors[x] for x in data[statuskey]]
    # Create a ColumnDataSource object
    source = ColumnDataSource(data)
    # Create a figure object
    plot = figure(height=350, title="Job Status", toolbar_location=None,
    tools="hover", tooltips="@process_status: @process_id")
    # Add the wedges to the plot
    plot.wedge(x=0, y=1, radius=0.4,
    start_angle=cumsum('angle', include_zero=True),
    end_angle=cumsum('angle'),
    line_color="white", fill_color='color', legend_field='process_status', source=source)
    # Remove the axis labels and grid lines
    plot.axis.axis_label = None
    plot.axis.visible = False
    plot.grid.grid_line_color = None
    curdoc().theme = 'dark_minimal'
    plot.background_fill_color = "white"
    plot.background_fill_alpha = 0
    plot.min_border_left = 1
    plot.min_border_right = 1
    plot.plot.min_border_top = 1
    plot.min_border_bottom = 1
    plot.border_fill_color = "white"
    plot.border_fill_alpha =  0
    plot.outline_line_width = 7
    plot.outline_line_width = 7
    plot.outline_line_alpha = 0
    plot.outline_line_color = "white"
    return components(plot)
    # Show the plot


async def plot_pie_chartX(inputdataset,idkey="process_id", statuskey='process_status', colors = {'Completed': 'green', 'Failed': 'red', 'In Progress': 'orange'}):
    dataset = pd.DataFrame.from_records(map(dict, inputdataset)).iloc[:, 1:]
    # Create a dictionary to map the status to a color
    #colors = {'Completed': 'green', 'Failed': 'red', 'In Progress': 'orange'}
    # Group the dataset by status
    data = dataset.groupby(statuskey).count().reset_index()
    # Add a column for the angle of each wedge
    data['angle'] = data[idkey]/data[idkey].sum() * 2*3.14
    # Add a column for the color of each wedge
    data['color'] = [colors[x] for x in data[statuskey]]
    # Create a ColumnDataSource object
    source = ColumnDataSource(data)
    # Create a figure object
    plot = figure(height=350, title="Job Status", toolbar_location=None,
    tools="hover", tooltips=f"@{statuskey}: @{idkey}")
    # Add the wedges to the plot
    plot.wedge(x=0, y=1, radius=0.4,
    start_angle=cumsum('angle', include_zero=True),
    end_angle=cumsum('angle'),
    line_color="white", fill_color='color', legend_field=statuskey, source=source)
    # Remove the axis labels and grid lines
    plot.axis.axis_label = None
    plot.axis.visible = False
    plot.grid.grid_line_color = None
    curdoc().theme = 'dark_minimal'
    plot.background_fill_color = "white"
    plot.background_fill_alpha = 0
    plot.min_border_left = 1
    plot.min_border_right = 1
    plot.plot.min_border_top = 1
    plot.min_border_bottom = 1
    plot.border_fill_color = "white"
    plot.border_fill_alpha =  0
    plot.outline_line_width = 7
    plot.outline_line_alpha = 0
    plot.outline_line_color = "white"
    plot.legend.background_fill_alpha = 0.0
    script, div = components(plot)
    soup = BeautifulSoup(div, 'html.parser')
    div_tag = soup.find('div')

    id_value = div_tag['id']
    data_root_id_value = div_tag['data-root-id']
    script = script.replace(data_root_id_value, "p0006")
    script = script.replace(id_value, "histplotguid")
    return script,div
    # Show the plot

def get_sesssion_for_transaction(dbid : str = "Remote",transaction_remote: AsyncSession = None,transaction_remote1: AsyncSession = None,transaction_local: AsyncSession= None):
    if dbid.lower() == "remote":
        return transaction_remote
    elif dbid.lower() == "remote1":
        return transaction_remote1
    elif dbid.lower() == "local":
        return transaction_local
    elif dbid.lower() == "savefile":
        return transaction_local



async def get_tables_list(
    request: Request, trans: AsyncSession, db_id: str, force_refresh: bool = False
) -> list[str]:

    trd = request.app.stores.get("tempdatard")
    tablelist_pickle = await trd.get(f"{db_id}_tables")
    if not tablelist_pickle or force_refresh == "true":
        context = {
            "t_dialect": static.SConstants.db_dilects[db_id],
            "db_name": static.SConstants.db_names[db_id],
            "schema_name": static.SConstants.schema_names[db_id],
        }
        sql_statement = (
            Template(template_name="sql/get_tables.txt", context=context)
            .to_asgi_response(request.app, request)
            .body.decode("utf-8")
        )
        sql_statement = str(sql_statement)
        print(sql_statement)
        result = await trans.execute(text(str(sql_statement)))
        table_list = [row[0] for row in result.all()]
        await trd.set(
            f"{db_id}_tables",
            pickle.dumps(table_list),
            expires_in=timedelta(seconds=500),
        )
    else:
        table_list = pickle.loads(tablelist_pickle)

    return table_list


async def get_clean_table_name(full_name: str) -> str:
    # split the string by the '.' character
    parts = full_name.split(".")
    # get the last part and remove the '[' and ']' characters
    table_name = parts[-1].strip("[]")
    return table_name


def read_batch_date_file() -> date:
    file_path: str = "/home/deck/devlopment/demo/batch_date.txt"
    with open(file_path, "r") as file:
        batch_date_str = file.read().strip()
        batch_date = pendulum.from_format(batch_date_str, "YYYYMMDD").date()
        return batch_date

async def validate_where(
    request: Request,
    trans: AsyncSession,
    db_id: str,
    table_name: str,
    column_list : list[dict[str, str]],
    additonal_where_clause: str,
) -> bool:
    context = {
            "t_dialect": static.SConstants.db_dilects[db_id],
            "db_name": static.SConstants.db_names[db_id],
            "schema_name": static.SConstants.schema_names[db_id],
            "table_name": await get_clean_table_name(table_name),
            "column_list": column_list,
            "valid_where_clause": additonal_where_clause
    }
    context["column_list"] = column_list
    sql_statement = (
        Template(template_name="sql/get_table_hash.txt", context=context)
        .to_asgi_response(request.app, request)
        .body.decode("utf-8")
    )
    try:
        result = await trans.execute(text(str(sql_statement)))
        ic(result.keys())
    except Exception as e:
        print(e)
        return False
    print(sql_statement)
    return True


async def get_column_list(
    request: Request,
    trans: AsyncSession,
    db_id: str,
    table_name: str,
    force_refresh: bool = False,
) -> list[dict[str, str]]:

    trd = request.app.stores.get("tempdatard")
    table_clm_list_pickle = await trd.get(f"{db_id}_{table_name}_columnlist")
    context = {}
    if not table_clm_list_pickle or force_refresh == "true":
        context = {
            "t_dialect": static.SConstants.db_dilects[db_id],
            "db_name": static.SConstants.db_names[db_id],
            "schema_name": static.SConstants.schema_names[db_id],
            "table_name": await get_clean_table_name(table_name),
        }
        sql_statement = (
            Template(template_name="sql/get_columns.txt", context=context)
            .to_asgi_response(request.app, request)
            .body.decode("utf-8")
        )
        sql_statement = str(sql_statement)
        print(sql_statement)
        result = await trans.execute(text(str(sql_statement)))

        
        column_list = [
            {"column_name": row[0], "column_type": row[1]} for row in result.all()
        ]
        await trd.set(
            f"{db_id}_{table_name}_columnlist",
            pickle.dumps(column_list),
            expires_in=timedelta(seconds=500),
        )
    else:
        column_list = pickle.loads(table_clm_list_pickle)
    context["column_list"] = column_list
    # sql_statement = (
    #     Template(template_name="sql/get_table_hash.txt", context=context)
    #     .to_asgi_response(request.app, request)
    #     .body.decode("utf-8")
    # )
    # print(sql_statement)
    return column_list




async def get_unique_col_list(
    request: Request,
    trans: AsyncSession,
    db_id: str,
    table_name: str,
    force_refresh: bool = False,
) -> list[dict[str, str]]:

    trd = request.app.stores.get("tempdatard")
    table_clm_list_pickle = await trd.get(f"{db_id}_{table_name}_uniqcolumnlist")
    context = {}
    if not table_clm_list_pickle or force_refresh == "true":
        context = {
            "t_dialect": static.SConstants.db_dilects[db_id],
            "db_name": static.SConstants.db_names[db_id],
            "schema_name": static.SConstants.schema_names[db_id],
            "table_name": await get_clean_table_name(table_name),
        }
        sql_statement = (
            Template(template_name="sql/get_primary_key.txt", context=context)
            .to_asgi_response(request.app, request)
            .body.decode("utf-8")
        )
        sql_statement = str(sql_statement)
        print(sql_statement)
        result = await trans.execute(text(str(sql_statement)))

        column_list_d = [
            row._asdict() for row in result.all()
        ]
        column_list = [row["Column_name"] for row in column_list_d]

        ic(column_list)
        await trd.set(
            f"{db_id}_{table_name}_uniqcolumnlist",
            pickle.dumps(column_list),
            expires_in=timedelta(seconds=500),
        )
    else:
        column_list = pickle.loads(table_clm_list_pickle)
    #context["column_list"] = column_list
    # sql_statement = (
    #     Template(template_name="sql/get_table_hash.txt", context=context)
    #     .to_asgi_response(request.app, request)
    #     .body.decode("utf-8")
    # )
    # print(sql_statement)
    return column_list






async def upsert_record(a_session: AsyncSession, record: T) -> T:
    a_session.add(record)
    await a_session.commit()
    return record

async def update_record(a_session: AsyncSession, record: SQLModel, pk: list[str]) -> T:
    fieldlist = [k for k, v in TCompareModel.__annotations__.items()]
    # Create a dictionary of key-value pairs for the primary key
    pk_values = {key: getattr(record, key) for key in pk}
    db_object = await a_session.get(type(record), pk_values)
    if db_object:
        
        model_deump_dict = record.model_dump(exclude_unset=True)
        for key, value in model_deump_dict.items():
            if key in fieldlist:
                setattr(db_object, key, value)
        a_session.add(db_object)
        await a_session.commit()
        #await a_session.refresh(db_object)
        return db_object
    else:
        for key in pk:
            if getattr(record, key) == 'None' or getattr(record, key) == '':
                setattr(record, key, None)
        ic(record)
        a_session.add(record)
        await a_session.commit()
        ic(record)
        return record



async def upsert_recordX(request :Request, a_session: AsyncSession, record: T) -> T:
    connect_args = {"check_same_thread": False}
    engine = create_engine(request.app.state['local_con_str'].replace('+aiosqlite',''), echo=True, connect_args=connect_args)
    with Session(engine) as session:
        rec = session.get(record, record.config_name)
        session.add(rec)
        session.commit()
        return record
    
async def upsert_records(a_session: AsyncSession, records: list[T]) -> list[T]:
    a_session.add_all(records)
    await a_session.commit()
    return records


async def process_compare_post(
    request: Request,
    data: dict,
    transaction_remote: AsyncSession,
    transaction_remote1: AsyncSession,
    transaction_local: AsyncSession,
    CLIENT_SESSION_ID: Optional[str],
    save_model : bool = False
):
    bdata = benedict(data)
    column_to_exclude = []  # New list to store keys starting with "exm"
    # Add keys starting with "exm" to column_to_exclude list
    column_to_exclude = [
        key.replace("exclm_", "") for key in bdata.keys() if key.startswith("exclm_")
    ]
    ic(column_to_exclude)
    unique_columns = []
    unique_columns = [
        key.replace("uniqclm_", "") for key in bdata.keys() if key.startswith("uniqclm_")
    ]




    force_refresh = False
    if not request.session.get("CLIENT_SESSION_ID"):
        request.session["CLIENT_SESSION_ID"] = CLIENT_SESSION_ID
        force_refresh = True
    benecontext = benedict()
    main_compare_types = {}
    ic(bdata)

    #data_compare_p = TCompareModel(**bdata)
    data_compare = TCompareModel(**bdata) 
    #data_compare = TCompareModel()

    


    if len(data_compare.columns_to_exclude_str) > 0:
        column_to_exclude = column_to_exclude + json_to_list(data_compare.columns_to_exclude_str)
    else:
        data_compare.columns_to_exclude_str = list_to_json(column_to_exclude)


    if len(data_compare.unique_columns_str) > 0:
        unique_columns = unique_columns + json_to_list(data_compare.unique_columns_str)
    else:
        data_compare.unique_columns_str = list_to_json(unique_columns)

    for t_types in TComareTypes:
        main_compare_types[t_types.name] = t_types.value

    columns_left: benedict = {}
    columns_right: benedict = {}
    common_columns: list = []
    unique_columns_left: list = []
    unique_columns_right: list = []

    if data_compare.left_db != "Select Database" and data_compare.left_db != "":
        trans_seession = get_sesssion_for_transaction(data_compare.left_db,transaction_remote,transaction_remote1)
        tables = await get_tables_list(
            request, trans_seession, data_compare.left_db, force_refresh
        )
        benecontext["common.left.tables"] = tables
    else:
        benecontext["common.left.tables"] = []

    if data_compare.right_db != "Select Database" and data_compare.right_db != "":
        trans_seession = get_sesssion_for_transaction(data_compare.right_db,transaction_remote,transaction_remote1)
        tables = await get_tables_list(
            request, trans_seession, data_compare.right_db, force_refresh
        )
        benecontext["common.right.tables"] = tables
    else:
        benecontext["common.right.tables"] = []

    if data_compare.left_tbl != "Select Table" and data_compare.left_tbl != "":
        trans_seession = get_sesssion_for_transaction(data_compare.left_db,transaction_remote,transaction_remote1)
        columns_left = await get_column_list(
            request, trans_seession, data_compare.left_db, data_compare.left_tbl
        )
        unique_columns_left = await get_unique_col_list(
            request, trans_seession, data_compare.left_db, data_compare.left_tbl
        )

    if data_compare.right_tbl != "Select Table" and data_compare.right_tbl != "":
        trans_seession = get_sesssion_for_transaction(data_compare.right_db,transaction_remote,transaction_remote1)
        columns_right = await get_column_list(
            request, trans_seession, data_compare.right_db, data_compare.right_tbl
        )
        unique_columns_right = await get_unique_col_list(
            request, trans_seession, data_compare.right_db, data_compare.right_tbl
        )

    unique_columns = list(set(unique_columns + unique_columns_left + unique_columns_right))
    data_compare.unique_columns_str = list_to_json(unique_columns)

    common_columns = []
    benecontext["common.left.columns"] = columns_left
    benecontext["common.right.columns"] = columns_right
    column_l_list = [column["column_name"] for column in columns_left]
    column_r_list = [column["column_name"] for column in columns_right]

    if len(columns_left) > 0 and len(columns_right) > 0:
        common_columns = [column for column in column_l_list if column in column_r_list]
        data_compare.common_columns_str = list_to_json(common_columns)


    
    message = ""
    ##Validation
    if (
        request.htmx
        and data_compare.left_tbl
        and data_compare.right_tbl
        and data_compare.right_tbl != "Select Table"
        and data_compare.right_tbl != ""
        and data_compare.left_tbl != "Select Table"
        and data_compare.left_tbl != ""
        and len(common_columns) == 0
    ):
        message = "No common columns found between the selected tables. Please select another table or database.<br>"


    #This is to validate the model and then save
    if save_model:
        message=""
        try:
            TCompareModel.model_validate(data_compare)
        except ValueError as e:
            if '[' in str(e):
               ic(str(e))
               msg = str(e).split('⚠️')[1].strip()
            else:
                msg = str(e)
            message = message + msg + "<br>"


                

 
        if len(columns_left) > 0 and len(data_compare.left_where_clause) > 0:
            trans_seession = get_sesssion_for_transaction(data_compare.left_db,transaction_remote,transaction_remote1)
            validate_where_clause = await validate_where(
                request, trans_seession, data_compare.left_db, data_compare.left_tbl, columns_left, data_compare.left_where_clause
            )
            if not validate_where_clause:
                message = message + "Left Additonal Where Clause is not valid.<br>"

        if len(columns_right) > 0 and len(data_compare.right_where_clause) > 0:
            trans_seession = get_sesssion_for_transaction(data_compare.right_db,transaction_remote,transaction_remote1)
            validate_where_clause = await validate_where(
                request, trans_seession, data_compare.right_db, data_compare.right_tbl, columns_right, data_compare.right_where_clause
            )
            if not validate_where_clause:
                message = message + "Right Additonal Where Clause is not valid.<br>"

        if not check_three_vars(data_compare.left_pivot_choice,data_compare.left_pivot_column,data_compare.left_pivot_format):
            message = message + "Left Pivot Choice, Pivot Column and Pivot Format should be all selected or all none of them should be used.<br>"
            if data_compare.left_pivot_choice != "":
                data_compare.left_pivot_choice = ""
            if data_compare.left_pivot_format != "":
                data_compare.left_pivot_format = ""
        else:
            if data_compare.left_pivot_column != "" and data_compare.left_pivot_column not in column_l_list:
                message = message + "Left Pivot Column is not in the selected table.<br>"

            else:
                if data_compare.left_pivot_choice == "custom" and data_compare.left_custom_pivot_value == "":
                    message = message + "Left Custom Pivot Value is required.<br>"

        if not check_three_vars(data_compare.right_pivot_choice,data_compare.right_pivot_column,data_compare.right_pivot_format):
            message = message + "Right Pivot Choice, Pivot Column and Pivot Format should be all selected or all none of them should be used.<br>"
            if data_compare.left_pivot_choice != "":
                data_compare.right_pivot_choice = ""
                if data_compare.right_pivot_format != "":
                    data_compare.right_pivot_format = ""
        else:
            if  data_compare.right_pivot_column != "" and data_compare.right_pivot_column not in column_r_list:
                message = message + "Right Pivot Column is not in the selected table.<br>"
            else:
                if data_compare.right_pivot_choice == "custom" and data_compare.right_custom_pivot_value == "":
                    message = message + "Right Custom Pivot Value is required.<br>"
        
        if message == "":
            ic(data_compare)
            if data_compare.left_pivot_format.lower().startswith("select"):
                data_compare.left_pivot_format = ""
            if data_compare.right_pivot_format.lower().startswith("select"):
                data_compare.right_pivot_format = ""

            data_compare = await update_record(transaction_local, data_compare, ["id"])
            message = "Model Saved Successfully"

    benecontext["toastmessage"] = message

    # Valdation Ends
    ic(CLIENT_SESSION_ID)
    benecontext["id"] = data_compare.id
    benecontext["config_name"] = data_compare.config_name
    benecontext["main_compare_types"] = main_compare_types
    benecontext["CLIENT_SESSION_ID"] = CLIENT_SESSION_ID
    benecontext["date_formats"] = static.SConstants.date_formats
    benecontext["db_names"] = static.SConstants.db_names.keys()
    benecontext["common_columns"] = data_compare.common_columns
    benecontext["column_to_exclude"] = data_compare.columns_to_exclude
    benecontext["unique_columns"] = data_compare.unique_columns

    try:
        benecontext["t_compare_objects"] = dict(data_compare)
    except Exception as e:
            jsonstr = data_compare.model_dump_json()
            json_dict = json.loads(jsonstr)
            benecontext["t_compare_objects"] = json_dict

    benecontext["common.left.db"] = data_compare.left_db
    benecontext["common.right.db"] = data_compare.right_db
    benecontext["common.left.tbl"] = data_compare.left_tbl
    benecontext["common.right.tbl"] = data_compare.right_tbl
    benecontext["common.left.pivot_choice"] = data_compare.left_pivot_choice
    benecontext["common.right.pivot_choice"] = data_compare.right_pivot_choice
    benecontext["common.left.pivot_column"] = data_compare.left_pivot_column
    benecontext["common.right.pivot_column"] = data_compare.right_pivot_column
    benecontext["common.left.pivot_format"] = data_compare.left_pivot_format
    benecontext["common.right.pivot_format"] = data_compare.right_pivot_format
    benecontext["common.left.where_clause"] = data_compare.left_where_clause
    benecontext["common.right.where_clause"] = data_compare.right_where_clause
    benecontext["common.left.custom_pivot_value"] = data_compare.left_custom_pivot_value
    benecontext["common.right.custom_pivot_value"] = data_compare.right_custom_pivot_value



    ic(data_compare)
    ic(benecontext)
    return benecontext
