import asyncio
import json
import pickle
from typing import Optional
from benedict import benedict
from litestar import Request
from loguru import logger
import pandas as pd
from sqlalchemy import text
from sqlmodel import Session, SQLModel, col, create_engine, select, and_, Session, or_
from lib.util import T, compare_mismatched_rows, find_mismatched_rows, get_common_rows, highlight_cells_with_value, highlight_positive_cells, json_to_list, list_to_string, list_to_json, check_three_vars, get_extra_rows_using_hash, make_header_background_grey
from models import remote
# sqlalchemy.ext.asyncio
# from sqlalchemy.ext.asyxxxxxxxxncio  import AsyncSexxxxxxxxxssion
from sqlmodel.ext.asyncio.session import AsyncSession
from datetime import date
import copy
from typing import TypeVar
from models.History import BaseHistory, SearchAndOrder, StatuSHistory
from models.TCompare import  TComareTypes, Tcomparemodel, Tschduledcomparerunmodel, CreateKeyTable
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
from litestar.channels import ChannelsPlugin
from openpyxl import load_workbook
from openpyxl.styles import PatternFill


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
    bscript: Optional[str] = "",
    bdiv: Optional[str] = "",
    documentpath="/",
) -> dict:
    return {
        "documentpath": documentpath,
        "title": title,
        "result_list": resultlist,
        "user_page_squeunce": hist.get_user_page_squeunce(),
        "sql_column_sequnce": hist.get_sql_column_sequnce(),
        "sql_order_sequnce": data.order_sequnce,
        "sql_order_direction": data.order_direction,
        "clm_name_mapping": static.SConstants.clm_name_mapping,
        "rev_direction": static.SConstants.rev_direction,
        "update_order_section": update_order_section,
        "histpiescript": bscript,
        "histpiechart": bdiv,
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
    result = await asyncsession.exec(text(str(sql_statement)))

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

    result = await transaction_remote.exec(query)
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
    result = await transaction_remote.exe(query)
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
    result = await transaction_remote.exe(query)
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
    result = await transaction_remote.exe(query)
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

    result = await transaction_remote.exe(query)
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

    result = await transaction_remote.exe(query)
    return result.all()

async def plot_pie_chart(inputdataset, idkey="process_id", statuskey="process_status"):
    dataset = pd.DataFrame.from_records(map(dict, inputdataset)).iloc[:, 1:]
    # Create a dictionary to map the status to a color
    colors = {"Completed": "green", "Failed": "red", "In Progress": "orange"}
    # Group the dataset by status
    data = dataset.groupby(statuskey).count().reset_index()
    # Add a column for the angle of each wedge
    data["angle"] = data[idkey] / data[idkey].sum() * 2 * 3.14
    # Add a column for the color of each wedge
    data["color"] = [colors[x] for x in data[statuskey]]
    # Create a ColumnDataSource object
    source = ColumnDataSource(data)
    # Create a figure object
    plot = figure(
        height=350,
        title="Job Status",
        toolbar_location=None,
        tools="hover",
        tooltips="@process_status: @process_id",
    )
    # Add the wedges to the plot
    plot.wedge(
        x=0,
        y=1,
        radius=0.4,
        start_angle=cumsum("angle", include_zero=True),
        end_angle=cumsum("angle"),
        line_color="white",
        fill_color="color",
        legend_field="process_status",
        source=source,
    )
    # Remove the axis labels and grid lines
    plot.axis.axis_label = None
    plot.axis.visible = False
    plot.grid.grid_line_color = None
    curdoc().theme = "dark_minimal"
    plot.background_fill_color = "white"
    plot.background_fill_alpha = 0
    plot.min_border_left = 1
    plot.min_border_right = 1
    plot.plot.min_border_top = 1
    plot.min_border_bottom = 1
    plot.border_fill_color = "white"
    plot.border_fill_alpha = 0
    plot.outline_line_width = 7
    plot.outline_line_width = 7
    plot.outline_line_alpha = 0
    plot.outline_line_color = "white"
    return components(plot)
    # Show the plot

async def plot_pie_chartX(
    inputdataset,
    idkey="process_id",
    statuskey="process_status",
    colors={"Completed": "green", "Failed": "red", "In Progress": "orange"},
):

    m = StatuSHistory(process_id=1, process_status="Completed")
    m1 = StatuSHistory(process_id=2, process_status="Failed")
    m2 = StatuSHistory(process_id=3, process_status="In Progress")
    m3 = StatuSHistory(process_id=4, process_status="In Progress")
    m4 = StatuSHistory(process_id=5, process_status="Completed")
    inputdataset.append(m)
    inputdataset.append(m1)
    inputdataset.append(m2)
    inputdataset.append(m3)
    inputdataset.append(m4)

    dataset = pd.DataFrame.from_records(map(dict, inputdataset)).iloc[:, 1:]

    # Create a dictionary to map the status to a color
    # colors = {'Completed': 'green', 'Failed': 'red', 'In Progress': 'orange'}
    # Group the dataset by status
    data = dataset.groupby(statuskey).count().reset_index()

    # Add a column for the angle of each wedge
    data["angle"] = data[idkey] / data[idkey].sum() * 2 * 3.14
    # Add a column for the color of each wedge
    data["color"] = [colors[x] for x in data[statuskey]]
    # Create a ColumnDataSource object
    source = ColumnDataSource(data)
    # Create a figure object
    plot = figure(
        height=350,
        title="Job Status",
        toolbar_location=None,
        tools="hover",
        tooltips=f"@{statuskey}: @{idkey}",
    )
    # Add the wedges to the plot
    plot.wedge(
        x=0,
        y=1,
        radius=0.4,
        start_angle=cumsum("angle", include_zero=True),
        end_angle=cumsum("angle"),
        line_color="white",
        fill_color="color",
        legend_field=statuskey,
        source=source,
    )
    # Remove the axis labels and grid lines
    plot.axis.axis_label = None
    plot.axis.visible = False
    plot.grid.grid_line_color = None
    curdoc().theme = "dark_minimal"
    plot.background_fill_color = "white"
    plot.background_fill_alpha = 0
    plot.min_border_left = 1
    plot.min_border_right = 1
    plot.plot.min_border_top = 1
    plot.min_border_bottom = 1
    plot.border_fill_color = "white"
    plot.border_fill_alpha = 0
    plot.outline_line_width = 7
    plot.outline_line_alpha = 0
    plot.outline_line_color = "white"
    plot.legend.background_fill_alpha = 0.0
    script, div = components(plot)
    soup = BeautifulSoup(div, "html.parser")
    div_tag = soup.find("div")

    id_value = div_tag["id"]
    data_root_id_value = div_tag["data-root-id"]
    script = script.replace(data_root_id_value, "p0006")
    script = script.replace(id_value, "histplotguid")
    return script, div
    # Show the plot

def get_sesssion_for_transaction(
    dbid: str = "Remote",
    transaction_remote: AsyncSession = None,
    transaction_remote1: AsyncSession = None,
    transaction_local: AsyncSession = None,
):
    if dbid.lower() == "remote":
        return transaction_remote
    elif dbid.lower() == "remote1":
        return transaction_remote1
    elif dbid.lower() == "local":
        return transaction_local
    elif dbid.lower() == "savefile":
        return transaction_local

def get_non_async_sesssion_for_transaction(
    dbid: str = "Remote",
    request: Request = None,
):
    if dbid.lower() == "remote":
        constring = request.app.state["remote_con_str"].replace("+aioodbc", "+pyodbc")
    elif dbid.lower() == "remote1":
        constring = request.app.state["remote_con_str1"].replace("+aioodbc", "+pyodbc")
    engine = create_engine(constring, echo=False)
    return engine

async def get_tables_list(
    request: Request, trans: AsyncSession, db_id: str, force_refresh: bool = False
) -> list[str]:
    trd = request.app.stores.get("memeory")
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
        result = await trans.exec(text(str(sql_statement)))
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

async def read_batch_date_file() -> date:
    file_path: str = "/home/deck/devlopment/litestardemo/batch_date.txt"
    with open(file_path, "r") as file:
        batch_date_str = file.read().strip()
        batch_date = pendulum.from_format(batch_date_str, "YYYYMMDD").date()
        return batch_date

async def validate_where(
    request: Request,
    trans: AsyncSession,
    db_id: str,
    table_name: str,
    column_list: list[dict[str, str]],
    additonal_where_clause: str,
) -> bool:
    context = {
        "t_dialect": static.SConstants.db_dilects[db_id],
        "db_name": static.SConstants.db_names[db_id],
        "schema_name": static.SConstants.schema_names[db_id],
        "table_name": await get_clean_table_name(table_name),
        "uniq_column_list": column_list,
        "column_list": column_list,
        "valid_where_clause": additonal_where_clause,
    }
    context["column_list"] = column_list
    sql_statement = (
        Template(template_name="sql/get_table_hash.txt", context=context)
        .to_asgi_response(request.app, request)
        .body.decode("utf-8")
    )
    try:
        result = await trans.exec(text(str(sql_statement)))
        # ic(result.keys())
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

    trd = request.app.stores.get("memory")
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
        result = await trans.exec(text(str(sql_statement)))

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

    trd = request.app.stores.get("memory")
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
        result = await trans.exec(text(str(sql_statement)))

        column_list_d = [row._asdict() for row in result.all()]
        column_list = [row["Column_name"] for row in column_list_d]

        # ic(column_list)
        await trd.set(
            f"{db_id}_{table_name}_uniqcolumnlist",
            pickle.dumps(column_list),
            expires_in=timedelta(seconds=500),
        )
    else:
        column_list = pickle.loads(table_clm_list_pickle)
    # context["column_list"] = column_list
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
    fieldlist = [k for k, v in Tcomparemodel.__annotations__.items()]
    # Create a dictionary of key-value pairs for the primary key
    pk_values = {key: getattr(record, key) for key in pk}
    db_object = await a_session.get(type(record), pk_values)
    if db_object:
        model_deump_dict = record.model_dump()
        #model_deump_dict = record.model_dump(exclude_unset=True)
        ic(model_deump_dict.items())
        for key, value in model_deump_dict.items():
            ic(key)
            if key in fieldlist:
                if key == 'left_pivot_choice':
                    ic(key)
                setattr(db_object, key, value)
        a_session.add(db_object)
        await a_session.commit()
        # await a_session.refresh(db_object)
        return db_object
    else:
        for key in pk:
            if getattr(record, key) == "None" or getattr(record, key) == "":
                setattr(record, key, None)
        # ic(record)
        a_session.add(record)
        await a_session.commit()
        # ic(record)
        return record

async def upsert_recordX(request: Request, a_session: AsyncSession, record: T) -> T:
    connect_args = {"check_same_thread": False}
    engine = create_engine(
        request.app.state["local_con_str"].replace("+aiosqlite", ""),
        echo=True,
        connect_args=connect_args,
    )
    with Session(engine) as session:
        rec = session.get(record, record.config_name)
        session.add(rec)
        session.commit()
        return record

async def upsert_records(a_session: AsyncSession, records: list[T]) -> list[T]:
    ic(records)
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
    save_model: bool = False,
):
    bdata = benedict(data)
    column_to_exclude = []  # New list to store keys starting with "exm"
    # Add keys starting with "exm" to column_to_exclude list
    column_to_exclude = [
        key.replace("exclm_", "") for key in bdata.keys() if key.startswith("exclm_")
    ]
    # ic(column_to_exclude)
    unique_columns = []
    unique_columns = [
        key.replace("uniqclm_", "")
        for key in bdata.keys()
        if key.startswith("uniqclm_")
    ]

    force_refresh = False
    if not request.session.get("CLIENT_SESSION_ID"):
        request.session["CLIENT_SESSION_ID"] = CLIENT_SESSION_ID
        force_refresh = True
    benecontext = benedict()
    main_compare_types = {}
    # ic(bdata)

    # data_compare_p = Tcomparemodel(**bdata)
    data_compare = Tcomparemodel(**bdata)
    # data_compare = Tcomparemodel()

    if len(data_compare.columns_to_exclude_str) > 0:
        column_to_exclude = column_to_exclude + json_to_list(
            data_compare.columns_to_exclude_str
        )
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

    (
        data_compare,
        columns_left,
        columns_right,
        unique_columns_left,
        unique_columns_right,
        unique_columns,
    ) = await process_db_tbl_columns(
        request,
        transaction_remote,
        transaction_remote1,
        unique_columns,
        force_refresh,
        benecontext,
        data_compare,
        columns_left,
        columns_right,
        unique_columns_left,
        unique_columns_right,
    )

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

    # This is to validate the model and then save
    if save_model:
        data_compare, message = await save_model_to_db(
            request,
            message,
            transaction_remote,
            transaction_remote1,
            transaction_local,
            data_compare,
            columns_left,
            columns_right,
            column_l_list,
            column_r_list,
        )

    benecontext["toastmessage"] = message

    # Valdation Ends
    # ic(CLIENT_SESSION_ID)
    benecontext["id"] = data_compare.id
    benecontext["config_name"] = data_compare.config_name
    benecontext["main_compare_types"] = main_compare_types
    benecontext["CLIENT_SESSION_ID"] = CLIENT_SESSION_ID
    benecontext["date_formats"] = static.SConstants.date_formats
    benecontext["db_names"] = static.SConstants.db_names.keys()
    benecontext["common_columns"] = data_compare.common_columns
    benecontext["column_to_exclude"] = data_compare.columns_to_exclude
    benecontext["unique_columns"] = data_compare.unique_columns

    # try:
    #     benecontext["t_compare_objects"] = dict(data_compare)
    # except Exception as e:
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
    benecontext["common.right.custom_pivot_value"] = (
        data_compare.right_custom_pivot_value
    )

    # ic(data_compare)
    # ic(benecontext)
    return benecontext

async def save_model_to_db(
    request,
    message,
    transaction_remote,
    transaction_remote1,
    transaction_local,
    data_compare,
    columns_left,
    columns_right,
    column_l_list,
    column_r_list,
):
    try:
        Tcomparemodel.model_validate(data_compare)
    except ValueError as e:
        if "[" in str(e):
            # ic(str(e))
            if "X" in str(e):
                msg = str(e).split("⚠️")[1].strip()
            else:
                msg = str(e).split("[")[0].replace("for Tcomparemodel", "")
        else:
            msg = str(e)
        message = message + msg + "<br>"

    if len(columns_left) > 0 and len(data_compare.left_where_clause) > 0:
        trans_seession = get_sesssion_for_transaction(
            data_compare.left_db, transaction_remote, transaction_remote1
        )
        validate_where_clause = await validate_where(
            request,
            trans_seession,
            data_compare.left_db,
            data_compare.left_tbl,
            columns_left,
            data_compare.left_where_clause,
        )
        if not validate_where_clause:
            message = message + "Left Additonal Where Clause is not valid.<br>"

    if len(columns_right) > 0 and len(data_compare.right_where_clause) > 0:
        trans_seession = get_sesssion_for_transaction(
            data_compare.right_db, transaction_remote, transaction_remote1
        )
        validate_where_clause = await validate_where(
            request,
            trans_seession,
            data_compare.right_db,
            data_compare.right_tbl,
            columns_right,
            data_compare.right_where_clause,
        )
        if not validate_where_clause:
            message = message + "Right Additonal Where Clause is not valid.<br>"

    if not check_three_vars(
        data_compare.left_pivot_choice,
        data_compare.left_pivot_column,
        data_compare.left_pivot_format,
    ):
        message = (
            message
            + "Left Pivot Choice, Pivot Column and Pivot Format should be all selected or all none of them should be used.<br>"
        )
        if data_compare.left_pivot_choice != "":
            data_compare.left_pivot_choice = ""
        if data_compare.left_pivot_format != "":
            data_compare.left_pivot_format = ""
    else:
        if (
            data_compare.left_pivot_column != ""
            and data_compare.left_pivot_column not in column_l_list
        ):
            message = message + "Left Pivot Column is not in the selected table.<br>"

        else:
            if (
                data_compare.left_pivot_choice == "custom"
                and data_compare.left_custom_pivot_value == ""
            ):
                message = message + "Left Custom Pivot Value is required.<br>"

    if not check_three_vars(
        data_compare.right_pivot_choice,
        data_compare.right_pivot_column,
        data_compare.right_pivot_format,
    ):
        message = (
            message
            + "Right Pivot Choice, Pivot Column and Pivot Format should be all selected or all none of them should be used.<br>"
        )
        if data_compare.right_pivot_choice != "":
            data_compare.right_pivot_choice = ""
        if data_compare.right_pivot_format != "":
            data_compare.right_pivot_format = ""
    else:
        if (
            data_compare.right_pivot_column != ""
            and data_compare.right_pivot_column not in column_r_list
        ):
            message = message + "Right Pivot Column is not in the selected table.<br>"
        else:
            if (
                data_compare.right_pivot_choice == "custom"
                and data_compare.right_custom_pivot_value == ""
            ):
                message = message + "Right Custom Pivot Value is required.<br>"

    if message == "":
        # ic(data_compare)
        if data_compare.left_pivot_format.lower().startswith("select"):
            data_compare.left_pivot_format = ""
        if data_compare.right_pivot_format.lower().startswith("select"):
            data_compare.right_pivot_format = ""

        if data_compare.left_custom_pivot_value:
            data_compare.left_custom_pivot_value = pendulum.parse(
                data_compare.left_custom_pivot_value
            ).date()

        if data_compare.right_custom_pivot_value:
            data_compare.right_custom_pivot_value = pendulum.parse(
                data_compare.right_custom_pivot_value
            ).date()

        data_compare = await update_record(transaction_local, data_compare, ["id"])
        message = "Model Saved Successfully"
    return data_compare, message

async def process_db_tbl_columns(
    request,
    transaction_remote,
    transaction_remote1,
    unique_columns,
    force_refresh,
    benecontext,
    data_compare,
    columns_left,
    columns_right,
    unique_columns_left,
    unique_columns_right,
):
    if data_compare.left_db != "Select Database" and data_compare.left_db != "":
        trans_seession = get_sesssion_for_transaction(
            data_compare.left_db, transaction_remote, transaction_remote1
        )
        tables = await get_tables_list(
            request, trans_seession, data_compare.left_db, force_refresh
        )
        benecontext["common.left.tables"] = tables
    else:
        benecontext["common.left.tables"] = []

    if data_compare.right_db != "Select Database" and data_compare.right_db != "":
        trans_seession = get_sesssion_for_transaction(
            data_compare.right_db, transaction_remote, transaction_remote1
        )
        tables = await get_tables_list(
            request, trans_seession, data_compare.right_db, force_refresh
        )
        benecontext["common.right.tables"] = tables
    else:
        benecontext["common.right.tables"] = []

    if data_compare.left_tbl != "Select Table" and data_compare.left_tbl != "" and data_compare.left_db != "Select Database":
        trans_seession = get_sesssion_for_transaction(
            data_compare.left_db, transaction_remote, transaction_remote1
        )
        columns_left = await get_column_list(
            request, trans_seession, data_compare.left_db, data_compare.left_tbl
        )
        unique_columns_left = await get_unique_col_list(
            request, trans_seession, data_compare.left_db, data_compare.left_tbl
        )

    if data_compare.right_tbl != "Select Table" and data_compare.right_tbl != "" and data_compare.right_db != "Select Database":
        trans_seession = get_sesssion_for_transaction(
            data_compare.right_db, transaction_remote, transaction_remote1
        )
        columns_right = await get_column_list(
            request, trans_seession, data_compare.right_db, data_compare.right_tbl
        )
        unique_columns_right = await get_unique_col_list(
            request, trans_seession, data_compare.right_db, data_compare.right_tbl
        )

    unique_columns = list(
        set(unique_columns + unique_columns_left + unique_columns_right)
    )
    data_compare.unique_columns_str = list_to_json(unique_columns)
    return (
        data_compare,
        columns_left,
        columns_right,
        unique_columns_left,
        unique_columns_right,
        unique_columns,
    )

# async def refresh_status_run_tbl(request:Request, asyncsession: AsyncSession,config_id: int,channels: ChannelsPlugin,sendone=False) -> None:
#     btlist = []
#     cur_last_run_id = 0
#     if not sendone:
#         trd = request.app.stores.get("memeory")
#         last_scr = await trd.get(f"last_scr_{config_id}")
#         if not last_scr:
#             last_scr = 0
#         ic(last_scr)
#         statement = select(Tschduledcomparerunmodel).where(col(Tschduledcomparerunmodel.id) > int(last_scr) , col(Tschduledcomparerunmodel.tcomparemodel_id) == config_id).order_by(Tschduledcomparerunmodel.id.desc()).limit(10)
#         run_statuses = await asyncsession.exec(statement)
#         bt = benedict()
#         for run_row in run_statuses.all():
#             if cur_last_run_id == 0:
#                 cur_last_run_id = run_row.id
#             btlist.append(run_row.model_dump())
#     else:
#         statement = select(Tschduledcomparerunmodel).where(col(Tschduledcomparerunmodel.tcomparemodel_id) == config_id).order_by(Tschduledcomparerunmodel.id.desc()).limit(1)
#         run_statuses = await asyncsession.exec(statement)
#         btlist.append(run_statuses.one().model_dump())
#     context = {
#         "config_run_id_tbl": btlist,
#         "config_id": config_id
#     }
#     fragment_response=Template(template_name='fragments/scr_table_row.html',context=context).to_asgi_response(request.app,request=request)

#     ic(fragment_response.body.decode('utf-8'))
#     if cur_last_run_id > 0 or sendone:
#         if channels:
#             channels.publish(fragment_response.body.decode('utf-8'),channels=[f'ws_chnl_compare_runs_{config_id}'])
#     if cur_last_run_id > 0:
#         await trd.set(f"last_scr_{config_id}", cur_last_run_id)


##There is better way to do this, above not working. I will update this later
async def refresh_status_run_tbl(
    request: Request,
    asyncsession: AsyncSession,
    config_id: int,
    channels: ChannelsPlugin,
    sendone=False,
) -> None:
    btlist = []
    statement = (
        select(Tschduledcomparerunmodel)
        .where(col(Tschduledcomparerunmodel.tcomparemodel_id) == config_id)
        .order_by(Tschduledcomparerunmodel.id.desc())
        .limit(10)
    )
    run_statuses = await asyncsession.exec(statement)
    for run_row in run_statuses.all():
        btlist.append(run_row.model_dump())
    context = {"config_run_id_tbl": btlist, "config_id": config_id}
    fragment_response = Template(
        template_name="fragments/scr_table_row.html", context=context
    ).to_asgi_response(request.app, request=request)
    ic(fragment_response.body.decode("utf-8"))
    if channels:
        channels.publish(
            fragment_response.body.decode("utf-8"),
            channels=[f"ws_chnl_compare_runs_{config_id}"],
        )

async def publish_log(request, channels, log_str, config_id, logtype="info"):
    if channels:
        if logtype == "info":
            channels.publish(
                f'<div id="terminal" hx-swap-oob="beforebegin:#terminallastrow"><pre data-prefix="$" hx-swap-oob="beforebegin:#terminallastrow"><code>{log_str}</code></pre></div>',
                channels=[f"ws_chnl_compare_runs_{config_id}"],
            )
        elif logtype == "warning":
            channels.publish(
                f'<div id="terminal" hx-swap-oob="beforebegin:#terminallastrow"><pre data-prefix="$" hx-swap-oob="beforebegin:#terminallastrow" class="bg-warning text-warning-content"><code>{log_str}</code></pre></div>',
                channels=[f"ws_chnl_compare_runs_{config_id}"],
            )

async def schedule_compare_run(
    request: Request,
    transaction_remote: AsyncSession,
    transaction_remote1: AsyncSession,
    transaction_local: AsyncSession,
    config_id: int,
    channels: ChannelsPlugin,
) -> None:
    try:
        l_tcomparemodel = await transaction_local.get(Tcomparemodel, config_id)

        statement = select(Tschduledcomparerunmodel).where(
            col(Tschduledcomparerunmodel.run_status) == "running",
            col(Tschduledcomparerunmodel.tcomparemodel_id) == config_id,
        )
        running_status = await transaction_local.exec(statement)
        if len(running_status.all()) > 0:
            await publish_log(
                request,
                channels,
                "Another run is in progress. Please wait for the current run to complete.",
                config_id,
                logtype="warning",
            )
            ic("Another run is in progress. Please wait for the current run to complete.")
            return

        l_tschduledcomparerunmodel = Tschduledcomparerunmodel(
            tcomparemodel=l_tcomparemodel,
            run_status="running",
            run_stat_time=pendulum.now("America/Toronto"),
        )
        transaction_local.add(l_tschduledcomparerunmodel)
        await transaction_local.commit()
        await publish_log(
            request,
            channels,
            f"run started for config id = {config_id}",
            config_id,
            logtype="info",
        )
        transaction_local.__init__(bind=transaction_local.bind)
        await refresh_status_run_tbl(
            request, transaction_local, config_id, channels, sendone=True
        )
        outputfilename = await compare_objects_main(
            l_tcomparemodel,
            request,
            transaction_remote,
            transaction_remote1,
            transaction_local,
            channels,
        )
        transaction_local.__init__(bind=transaction_local.bind)
        l_tschduledcomparerunmodel.run_status = "completed"
        l_tschduledcomparerunmodel.run_end_time = pendulum.now("America/Toronto")
        l_tschduledcomparerunmodel.run_report = outputfilename
        transaction_local.add(l_tschduledcomparerunmodel)
        await transaction_local.commit()
        transaction_local.__init__(bind=transaction_local.bind)
        await refresh_status_run_tbl(
            request, transaction_local, config_id, channels, sendone=True
        )
        await publish_log(
            request,
            channels,
            f"run completed for config id = {config_id}",
            config_id,
            logtype="info",
        )
    except Exception as e:
        l_tschduledcomparerunmodel.run_status = "error"
        transaction_local.add(l_tschduledcomparerunmodel)
        await transaction_local.commit()
        await publish_log(
            request,
            channels,
            f"An error occurred during the run for config id = {config_id}: {str(e)}",
            config_id,
            logtype="warning",
        )
        transaction_local.__init__(bind=transaction_local.bind)
        await refresh_status_run_tbl(
            request, transaction_local, config_id, channels, sendone=True
        )
    # asyncio.create_task(publish_log(request,channels,f'run completed for config id = {config_id}',config_id,logtype="info"))

async def compare_objects_main(
    comparemodel: Tcomparemodel,
    request: Request,
    transaction_remote: AsyncSession,
    transaction_remote1: AsyncSession,
    transaction_local: AsyncSession,
    channels: ChannelsPlugin
) -> str:
    filename = f"{comparemodel.config_name}_{comparemodel.id}_compare_{pendulum.now('America/Toronto').format('YYYYMMDDHHmmss')}"
    if comparemodel.left_obj_type == "Table":
        trans_seession_left = get_sesssion_for_transaction(
            comparemodel.left_db, transaction_remote, transaction_remote1
        )
        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_left,'left')
        print(sql_statement)
        result = await trans_seession_left.exec(text(sql_statement)) 
        hash_df_left = pd.DataFrame(result.fetchall())
        ic(hash_df_left)
        

    if comparemodel.right_obj_type == "Table":
        trans_seession_right = get_sesssion_for_transaction(
            comparemodel.right_db, transaction_remote, transaction_remote1
        )
        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_right,'right')
        print(sql_statement)
        result = await trans_seession_right.exec(text(sql_statement))
        hash_df_right = pd.DataFrame(result.fetchall())
        ic(hash_df_right)

    #==============
    full_count_first=len(hash_df_left)
    full_count_second=len(hash_df_right)
    await publish_log(request=request,channels=channels,log_str=f'Count for {comparemodel.left_tbl} in {comparemodel.left_db} is {full_count_first}',config_id=comparemodel.id,logtype="info")
    await publish_log(request=request,channels=channels,log_str=f'Count for {comparemodel.right_tbl} in {comparemodel.right_db} is {full_count_second}',config_id=comparemodel.id,logtype="info")
    df_matching = pd.merge(hash_df_left, hash_df_right, on=['ConcatenatedKeys', 'HashValue'], how='inner')
    await publish_log(request=request,channels=channels,log_str=f'Common record Count is {len(df_matching)}',config_id=comparemodel.id,logtype="info")
    await publish_log(request=request,channels=channels,log_str=f'Calculating mismatched records between two dataframes',config_id=comparemodel.id,logtype="info")
    merged_df = pd.merge(hash_df_left, hash_df_right, on='ConcatenatedKeys', how='outer', suffixes=('_df_left', '_df_right'))
    diff_df = merged_df[merged_df['HashValue_df_left'] != merged_df['HashValue_df_right']][['ConcatenatedKeys', 'HashValue_df_left', 'HashValue_df_right']]    
    ic(diff_df)
    
    hash_mismatch_all_cols_df_left = pd.DataFrame(columns=['ConcatenatedKeys'])
    hash_mismatch_all_cols_df_right = pd.DataFrame(columns=['ConcatenatedKeys'])
    key_tbl_pd1 = pd.DataFrame(columns=['ConcatenatedKeys'])
    key_tbl_pd2 = pd.DataFrame(columns=['ConcatenatedKeys'])
    # mismatch_common = pd.DataFrame(columns=['clmid'])
    # extra_first = pd.DataFrame(columns=['clmid'])
    # extra_second = pd.DataFrame(columns=['clmid'])
    trd = request.app.stores.get("memory")
    compare_limit = await trd.get(f"compare_limit")
    if not compare_limit:
        compare_limit = 1000
    
    if len(diff_df) > 0:
        key_tbl_pd1=hash_df_left[hash_df_left['ConcatenatedKeys'].isin(diff_df['ConcatenatedKeys'])]['ConcatenatedKeys']
        key_tbl_pd2=hash_df_right[hash_df_right['ConcatenatedKeys'].isin(diff_df['ConcatenatedKeys'])]['ConcatenatedKeys']

        #left_non_async_engine = get_non_async_sesssion_for_transaction(comparemodel.left_db,request)
        left_sql_remove_table_for_hashstore = get_remove_sql_for_hashstore(comparemodel.left_db,comparemodel.left_tbl,request,'_left')
        await trans_seession_left.exec(text(left_sql_remove_table_for_hashstore))
        left_sql_create_table_for_hashstore = get_create_sql_for_hashstore(comparemodel.left_db,comparemodel.left_tbl,request,'_left')
        await trans_seession_left.exec(text(left_sql_create_table_for_hashstore))
        #key_tbl_pd1[:compare_limit].to_sql(f'{comparemodel.left_tbl}_left_key_tmp',con=left_non_async_engine,if_exists='replace', index=False)
        key_tbl_model_left = CreateKeyTable(f'{comparemodel.left_tbl}_left_key_tmp')
        key_obj_list_left : list[SQLModel] = []
        for row in key_tbl_pd1[:compare_limit]:
            key_tbl_model_left_obj =  key_tbl_model_left(ConcatenatedKeys=row)
            key_obj_list_left.append(key_tbl_model_left_obj)
        trans_seession_left.add_all(key_obj_list_left)
        await trans_seession_left.commit()
        trans_seession_left.__init__(bind=trans_seession_left.bind)


        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_left,'left','yes')
        print(sql_statement)
        result = await trans_seession_left.exec(text(sql_statement)) 
        hash_mismatch_all_cols_df_left = pd.DataFrame(result.fetchall())
        await publish_log(request=request,channels=channels,log_str="Hash mismatch all columns for left table calculated.",config_id=comparemodel.id,logtype="info")


        #right_non_async_engine = get_non_async_sesssion_for_transaction(comparemodel.right_db,request)
        right_sql_remove_table_for_hashstore = get_remove_sql_for_hashstore(comparemodel.right_db,comparemodel.right_tbl,request,'_right')
        await trans_seession_right.exec(text(right_sql_remove_table_for_hashstore))
        right_sql_create_table_for_hashstore = get_create_sql_for_hashstore(comparemodel.right_db,comparemodel.right_tbl,request,'_right')
        await trans_seession_right.exec(text(right_sql_create_table_for_hashstore))
        #key_tbl_pd2[:compare_limit].to_sql(f'{comparemodel.right_tbl}_right_key_tmp',con=right_non_async_engine,if_exists='replace', index=False)
        key_tbl_model_right = CreateKeyTable(f'{comparemodel.right_tbl}_right_key_tmp')
        key_obj_list_right : list[SQLModel] = []
        for row in key_tbl_pd2[:compare_limit]:
            key_tbl_model_right_obj =  key_tbl_model_right(ConcatenatedKeys=row)
            key_obj_list_right.append(key_tbl_model_right_obj)
        trans_seession_right.add_all(key_obj_list_right)
        await trans_seession_right.commit()
        trans_seession_right.__init__(bind=trans_seession_left.bind)


        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_right,'right','yes')
        print(sql_statement)
        result = await trans_seession_right.exec(text(sql_statement)) 
        hash_mismatch_all_cols_df_right = pd.DataFrame(result.fetchall())
        await publish_log(request=request,channels=channels,log_str="Hash mismatch all columns for right table calculated.",config_id=comparemodel.id,logtype="info")
        
    

    #transaction_remote.__init__(bind=transaction_remote.bind)

    #===============
    df_left_extra,df_right_extra = get_extra_rows_using_hash(hash_df_left, hash_df_right)
    ic(df_left_extra)
    ic(df_right_extra)


    mismatched_df = find_mismatched_rows(
        hash_df_left[["ConcatenatedKeys", "HashValue"]].copy(),
        hash_df_right[["ConcatenatedKeys", "HashValue"]].copy(),
    )
    ic(hash_mismatch_all_cols_df_left)
    mismatched_dict = compare_mismatched_rows(hash_mismatch_all_cols_df_left, hash_mismatch_all_cols_df_right, mismatched_df,df_left_extra,df_right_extra)
    ic(mismatched_dict)
    await publish_log(request=request,channels=channels,log_str="Mismatched rows calculated.",config_id=comparemodel.id,logtype="info")


    passed_col_df = pd.DataFrame(columns=['column_name', 'ConcatenatedKeys', 'value_df_left', 'value_df_right'])
    ic(passed_col_df)

    if len(diff_df) > 0:
        passed_table_sql_template = "sql/get_passed_table_rows.txt"
    else:
        passed_table_sql_template = "sql/get_passed_table_rows_success.txt"

    passed_sql_statement_left = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_left,'left','yes',passed_table_sql_template)
    result = await trans_seession_left.exec(text(passed_sql_statement_left)) 
    hash_passed_df_left = pd.DataFrame(result.fetchall())
    ic(hash_passed_df_left)
    if len(diff_df) > 0:
        await trans_seession_left.exec(text(left_sql_remove_table_for_hashstore))

    passed_sql_statement_right = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_right,'right','yes',passed_table_sql_template)
    result = await trans_seession_right.exec(text(passed_sql_statement_right)) 
    hash_passed_df_right = pd.DataFrame(result.fetchall())
    ic(hash_passed_df_right)
    if len(diff_df) > 0:
        await trans_seession_right.exec(text(right_sql_remove_table_for_hashstore))
    
    common_df = get_common_rows(hash_passed_df_left, hash_passed_df_right, ["ConcatenatedKeys"])
    common_df.set_index('ConcatenatedKeys', inplace=False)
    ic(common_df)
    await publish_log(request=request,channels=channels,log_str="Common rows calculated.",config_id=comparemodel.id,logtype="info")

    passed_benedict_list : list[benedict] = []

    for column in hash_passed_df_left.columns:
        #ic(column)
        for index, common_row in common_df.head(10).iterrows():
                    # New row as a DataFrame
            if column in ['ConcatenatedKeys','HashValue']:
                continue
            item =  benedict()
            item.column_name = column
            item.ConcatenatedKeys = common_row.ConcatenatedKeys
            item.value_df_left = common_row[f'{column}_x']
            item.value_df_right = common_row[f'{column}_y']
            passed_benedict_list.append(item)
    passed_col_df = pd.DataFrame(passed_benedict_list)
    path = f'/home/deck/Downloads'
    file = f'{path}/{filename}.xlsx'
    with pd.ExcelWriter(file, engine="xlsxwriter") as writer:
        # Create a new dataframe with the column names from df_left
        summary_df = pd.DataFrame({"ColumnName": hash_passed_df_left.columns})

        # Write the dataframe to the Excel sheet
        summary_df["MatchedRowsCountLeft"] = summary_df["ColumnName"].apply(
            lambda col: len(hash_df_left)
            - len(df_left_extra)
            - ((len(mismatched_dict[col]) if col in mismatched_dict else 0) - (len(df_left_extra)+len(df_right_extra)))
        )

        summary_df["MatchedRowsCountRight"] = summary_df["ColumnName"].apply(
            lambda col: len(hash_df_right)
            - len(df_right_extra)
            - ((len(mismatched_dict[col]) if col in mismatched_dict else 0) - (len(df_left_extra)+len(df_right_extra)))
        )


        summary_df["MisMatchedRowsCount"] = summary_df["ColumnName"].apply(
            lambda col: (len(mismatched_dict[col]) if col in mismatched_dict else 0)
        )

        summary_df["ExtraInLeft"] = summary_df["ColumnName"].apply(
            lambda col: len(df_left_extra)
        )
        summary_df["ExtraInRight"] = summary_df["ColumnName"].apply(
            lambda col: len(df_right_extra)
        )

        summary_df["TotalRowsInLeft"] = len(hash_df_left)
        summary_df["TotalRowsInRight"] = len(hash_df_right)

        
        summary_df = summary_df.drop([0, 1])
        meta_df = pd.DataFrame(columns=['ConfigName', 'ConfigValue'])
        meta_df.loc[0] = ['LeftDatabaseName', static.SConstants.db_names[comparemodel.left_db]]
        meta_df.loc[1] = ['LeftTableName', comparemodel.left_tbl]
        meta_df.loc[2] = ['RightDatabaseName', static.SConstants.db_names[comparemodel.right_db]]
        meta_df.loc[3] = ['RightTableName', comparemodel.right_tbl]
        meta_df.loc[4] = ['UniqueColumns', comparemodel.unique_columns]
        meta_df.loc[5] = ['ExcludedColumns', comparemodel.columns_to_exclude]


        meta_df.to_excel(writer, sheet_name="MetaInfo", index=False)

        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        passed_col_df.to_excel(writer, sheet_name="PassRecordSummary", index=False)

        
        
        for column_name, differ_items in mismatched_dict.items():
            mismatched_df = pd.DataFrame(columns=['ConcatenatedKeys', 'value_df_left', 'value_df_right'])
            mismatched_bendict_list : list[benedict] = []
            for dit in differ_items:
                item =  benedict()
                item.ConcatenatedKeys = dit.ConcatenatedKeys
                item.value_df_left = dit.df_left_value
                item.value_df_right = dit.df_right_value
                mismatched_bendict_list.append(item)
            mismatched_df = pd.DataFrame(mismatched_bendict_list)

            ic(df_left_extra)
            mismatched_df.to_excel(writer, sheet_name=f"{column_name}_miss", index=False)
    await highlight_positive_cells(file,'Summary','MisMatchedRowsCount')
    await highlight_positive_cells(file,'Summary','ExtraInLeft')
    await highlight_positive_cells(file,'Summary','ExtraInRight')
    await highlight_cells_with_value(file,'Does_Not_Exists')
    await make_header_background_grey(file)
    await publish_log(request=request,channels=channels,log_str="Excel file created.",config_id=comparemodel.id,logtype="info")
    return f"{filename}.xlsx"



def get_sql_for_hashstore(db_id,table_name,request:Request,template_name,side):
    schema_name = static.SConstants.schema_names[db_id]
    db_name = static.SConstants.db_names[db_id]
    t_dialect = static.SConstants.db_dilects[db_id]
    l_context = {
            "t_dialect":static.SConstants.db_dilects[db_id],
            "db_name": static.SConstants.db_names[db_id],
            "schema_name": static.SConstants.schema_names[db_id],
            "table_name":table_name,
            "side":side
        }
    sql_statement = (
            Template(template_name=template_name, context=l_context)
            .to_asgi_response(request.app, request)
            .body.decode("utf-8")
        )
    return sql_statement

def get_create_sql_for_hashstore(db_id,table_name,request:Request,side):
    return get_sql_for_hashstore(db_id,table_name,request,"sql/add_key_tbl.txt",side)
    
def get_remove_sql_for_hashstore(db_id,table_name,request:Request,side):
    return get_sql_for_hashstore(db_id,table_name,request,"sql/rm_key_tbl.txt",side)
    
async def get_left_tbl_full_hash_sql(comparemodel: Tcomparemodel, request, trans_seession,select_other_columns='no'):

    columns_left = await get_column_list(
            request, trans_seession, comparemodel.left_db, comparemodel.left_tbl
        )
    column_l_list = [
            column
            for column in columns_left
            if column["column_name"] in comparemodel.common_columns
            and column["column_name"] not in comparemodel.columns_to_exclude
        ]
    column_lu_list = [
            column
            for column in columns_left
            if column["column_name"] in comparemodel.unique_columns
        ]
    pivot_dt_str = ""
    if comparemodel.left_pivot_choice:
        pivot_dt_str = ""
        if comparemodel.left_pivot_choice == "Batch":
            batch_dt = await read_batch_date_file()
            pivot_dt_str = pendulum.instance(batch_dt).format(
                    comparemodel.left_pivot_format
                )
        elif comparemodel.left_pivot_choice == "Current":
            pivot_dt_str = pendulum.now("America/Toronto").format(
                    comparemodel.left_pivot_format
                )
        elif comparemodel.left_pivot_choice == "Custom":
            pivot_dt_str = pendulum.instance(
                    comparemodel.left_custom_pivot_value
                ).format(comparemodel.left_pivot_format)

    hashcontext = {
            "t_dialect": static.SConstants.db_dilects[comparemodel.left_db],
            "db_name": static.SConstants.db_names[comparemodel.left_db],
            "schema_name": static.SConstants.schema_names[comparemodel.left_db],
            "table_name": await get_clean_table_name(comparemodel.left_tbl),
            "column_list": column_l_list,
            "uniq_column_list": column_lu_list,
            "actul_where_cluase": comparemodel.left_where_clause,
            "pivot_column": comparemodel.left_pivot_column,
            "pivot_dt_str": pivot_dt_str,
            "select_other_columns" : select_other_columns
        }
    sql_statement = (
            Template(template_name="sql/get_table_hash.txt", context=hashcontext)
            .to_asgi_response(request.app, request)
            .body.decode("utf-8")
        )
    return sql_statement

async def get_right_tbl_full_hash_sql(comparemodel: Tcomparemodel, request, trans_seession,select_other_columns='no'):

    columns_right = await get_column_list(
            request, trans_seession, comparemodel.right_db, comparemodel.right_tbl
        )
    column_l_list = [
            column
            for column in columns_right
            if column["column_name"] in comparemodel.common_columns
            and column["column_name"] not in comparemodel.columns_to_exclude
        ]
    column_lu_list = [
            column
            for column in columns_right
            if column["column_name"] in comparemodel.unique_columns
        ]
    pivot_dt_str = ""
    if comparemodel.right_pivot_choice:
        pivot_dt_str = ""
        if comparemodel.right_pivot_choice == "Batch":
            batch_dt = await read_batch_date_file()
            pivot_dt_str = pendulum.instance(batch_dt).format(
                    comparemodel.right_pivot_format
                )
        elif comparemodel.right_pivot_choice == "Current":
            pivot_dt_str = pendulum.now("America/Toronto").format(
                    comparemodel.right_pivot_format
                )
        elif comparemodel.right_pivot_choice == "Custom":
            pivot_dt_str = pendulum.instance(
                    comparemodel.right_custom_pivot_value
                ).format(comparemodel.right_pivot_format)

    hashcontext = {
            "t_dialect": static.SConstants.db_dilects[comparemodel.right_db],
            "db_name": static.SConstants.db_names[comparemodel.right_db],
            "schema_name": static.SConstants.schema_names[comparemodel.right_db],
            "table_name": await get_clean_table_name(comparemodel.right_tbl),
            "column_list": column_l_list,
            "uniq_column_list": column_lu_list,
            "actul_where_cluase": comparemodel.right_where_clause,
            "pivot_column": comparemodel.right_pivot_column,
            "pivot_dt_str": pivot_dt_str,
            "select_other_columns" : select_other_columns
        }
    sql_statement = (
            Template(template_name="sql/get_table_hash.txt", context=hashcontext)
            .to_asgi_response(request.app, request)
            .body.decode("utf-8")
        )
    return sql_statement

async def get_tbl_full_hash_sql(
    comparemodel: Tcomparemodel,
    request,
    trans_seession,
    compare_side: str,
    select_other_columns='no',
    template_name="sql/get_table_hash.txt",
):
    if compare_side == "left":
        columns = await get_column_list(
            request, trans_seession, comparemodel.left_db, comparemodel.left_tbl
        )
        pivot_choice = comparemodel.left_pivot_choice
        pivot_column = comparemodel.left_pivot_column
        pivot_format = comparemodel.left_pivot_format
        where_clause = comparemodel.left_where_clause
        custom_pivot_value = comparemodel.left_custom_pivot_value
        clean_tbl_name = await get_clean_table_name(comparemodel.left_tbl)
        db = comparemodel.left_db
    elif compare_side == "right":
        columns = await get_column_list(
            request, trans_seession, comparemodel.right_db, comparemodel.right_tbl
        )
        pivot_choice = comparemodel.right_pivot_choice
        pivot_column = comparemodel.right_pivot_column
        pivot_format = comparemodel.right_pivot_format
        where_clause = comparemodel.right_where_clause
        custom_pivot_value = comparemodel.right_custom_pivot_value
        clean_tbl_name = await get_clean_table_name(comparemodel.right_tbl)
        db = comparemodel.right_db
    else:
        raise ValueError("Invalid table_type")

    column_list = [
        column
        for column in columns
        if column["column_name"] in comparemodel.common_columns
        and column["column_name"] not in comparemodel.columns_to_exclude
    ]
    uniq_column_list = [
        column
        for column in columns
        if column["column_name"] in comparemodel.unique_columns
    ]
    pivot_dt_str = ""
    if pivot_choice:
        pivot_dt_str = ""
        if pivot_choice == "Batch":
            batch_dt = await read_batch_date_file()
            pivot_dt_str = pendulum.instance(batch_dt).format(pivot_format)
        elif pivot_choice == "Current":
            pivot_dt_str = pendulum.now("America/Toronto").format(pivot_format)
        elif pivot_choice == "Custom":
            pivot_dt_str = pendulum.instance(custom_pivot_value).format(pivot_format)

    hashcontext = {
        "t_dialect": static.SConstants.db_dilects[db],
        "db_name": static.SConstants.db_names[db],
        "schema_name": static.SConstants.schema_names[db],
        "table_name": clean_tbl_name,
        "column_list": column_list,
        "uniq_column_list": uniq_column_list,
        "actul_where_cluase": where_clause,
        "pivot_column": pivot_column,
        "pivot_dt_str": pivot_dt_str,
        "select_other_columns": select_other_columns,
        "side": f"_{compare_side}",
    }
    sql_statement = (
        Template(template_name=template_name, context=hashcontext)
        .to_asgi_response(request.app, request)
        .body.decode("utf-8")
    )
    return sql_statement



async def compare_objects_main_new(
    comparemodel: Tcomparemodel,
    request: Request,
    transaction_remote: AsyncSession,
    transaction_remote1: AsyncSession,
    transaction_local: AsyncSession,
    channels: ChannelsPlugin,
    run_id: int = 6
) -> str:
    filename = f"{comparemodel.config_name}_{comparemodel.id}_compare_{pendulum.now('America/Toronto').format('YYYYMMDDHHmmss')}"
    if comparemodel.left_obj_type == "Table":
        trans_seession_left = get_sesssion_for_transaction(
            comparemodel.left_db, transaction_remote, transaction_remote1
        )
        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_left,'left')
        print(sql_statement)
        result = await trans_seession_left.exec(text(sql_statement)) 
        hash_df_left = pd.DataFrame(result.fetchall())
        ic(hash_df_left)
        

    if comparemodel.right_obj_type == "Table":
        trans_seession_right = get_sesssion_for_transaction(
            comparemodel.right_db, transaction_remote, transaction_remote1
        )
        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_right,'right')
        print(sql_statement)
        result = await trans_seession_right.exec(text(sql_statement))
        hash_df_right = pd.DataFrame(result.fetchall())
        ic(hash_df_right)

    #==============
    full_count_first=len(hash_df_left)
    full_count_second=len(hash_df_right)
    await publish_log(request=request,channels=channels,log_str=f'Count for {comparemodel.left_tbl} in {comparemodel.left_db} is {full_count_first}',config_id=comparemodel.id,logtype="info")
    await publish_log(request=request,channels=channels,log_str=f'Count for {comparemodel.right_tbl} in {comparemodel.right_db} is {full_count_second}',config_id=comparemodel.id,logtype="info")
    df_matching = pd.merge(hash_df_left, hash_df_right, on=['ConcatenatedKeys', 'HashValue'], how='inner')
    await publish_log(request=request,channels=channels,log_str=f'Common record Count is {len(df_matching)}',config_id=comparemodel.id,logtype="info")
    await publish_log(request=request,channels=channels,log_str=f'Calculating mismatched records between two dataframes',config_id=comparemodel.id,logtype="info")
    merged_df = pd.merge(hash_df_left, hash_df_right, on='ConcatenatedKeys', how='outer', suffixes=('_df_left', '_df_right'))
    diff_df = merged_df[merged_df['HashValue_df_left'] != merged_df['HashValue_df_right']][['ConcatenatedKeys', 'HashValue_df_left', 'HashValue_df_right']]    
    ic(diff_df)
    
    hash_mismatch_all_cols_df_left = pd.DataFrame(columns=['ConcatenatedKeys'])
    hash_mismatch_all_cols_df_right = pd.DataFrame(columns=['ConcatenatedKeys'])
    key_tbl_pd1 = pd.DataFrame(columns=['ConcatenatedKeys'])
    key_tbl_pd2 = pd.DataFrame(columns=['ConcatenatedKeys'])
    # mismatch_common = pd.DataFrame(columns=['clmid'])
    # extra_first = pd.DataFrame(columns=['clmid'])
    # extra_second = pd.DataFrame(columns=['clmid'])
    trd = request.app.stores.get("memory")
    compare_limit = await trd.get(f"compare_limit")
    if not compare_limit:
        compare_limit = 1000
    
    if len(diff_df) > 0:
        key_tbl_pd1=hash_df_left[hash_df_left['ConcatenatedKeys'].isin(diff_df['ConcatenatedKeys'])]['ConcatenatedKeys']
        key_tbl_pd2=hash_df_right[hash_df_right['ConcatenatedKeys'].isin(diff_df['ConcatenatedKeys'])]['ConcatenatedKeys']

        #left_non_async_engine = get_non_async_sesssion_for_transaction(comparemodel.left_db,request)
        left_sql_remove_table_for_hashstore = get_remove_sql_for_hashstore(comparemodel.left_db,comparemodel.left_tbl,request,'_left')
        await trans_seession_left.exec(text(left_sql_remove_table_for_hashstore))
        left_sql_create_table_for_hashstore = get_create_sql_for_hashstore(comparemodel.left_db,comparemodel.left_tbl,request,'_left')
        await trans_seession_left.exec(text(left_sql_create_table_for_hashstore))
        #key_tbl_pd1[:compare_limit].to_sql(f'{comparemodel.left_tbl}_left_key_tmp',con=left_non_async_engine,if_exists='replace', index=False)
        key_tbl_model_left = CreateKeyTable(f'{comparemodel.left_tbl}_left_key_tmp')
        key_obj_list_left : list[SQLModel] = []
        for row in key_tbl_pd1[:compare_limit]:
            key_tbl_model_left_obj =  key_tbl_model_left(ConcatenatedKeys=row)
            key_obj_list_left.append(key_tbl_model_left_obj)
        trans_seession_left.add_all(key_obj_list_left)
        await trans_seession_left.commit()
        trans_seession_left.__init__(bind=trans_seession_left.bind)


        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_left,'left','yes')
        print(sql_statement)
        result = await trans_seession_left.exec(text(sql_statement)) 
        hash_mismatch_all_cols_df_left = pd.DataFrame(result.fetchall())
        await publish_log(request=request,channels=channels,log_str="Hash mismatch all columns for left table calculated.",config_id=comparemodel.id,logtype="info")


        #right_non_async_engine = get_non_async_sesssion_for_transaction(comparemodel.right_db,request)
        right_sql_remove_table_for_hashstore = get_remove_sql_for_hashstore(comparemodel.right_db,comparemodel.right_tbl,request,'_right')
        await trans_seession_right.exec(text(right_sql_remove_table_for_hashstore))
        right_sql_create_table_for_hashstore = get_create_sql_for_hashstore(comparemodel.right_db,comparemodel.right_tbl,request,'_right')
        await trans_seession_right.exec(text(right_sql_create_table_for_hashstore))
        #key_tbl_pd2[:compare_limit].to_sql(f'{comparemodel.right_tbl}_right_key_tmp',con=right_non_async_engine,if_exists='replace', index=False)
        key_tbl_model_right = CreateKeyTable(f'{comparemodel.right_tbl}_right_key_tmp')
        key_obj_list_right : list[SQLModel] = []
        for row in key_tbl_pd2[:compare_limit]:
            key_tbl_model_right_obj =  key_tbl_model_right(ConcatenatedKeys=row)
            key_obj_list_right.append(key_tbl_model_right_obj)
        trans_seession_right.add_all(key_obj_list_right)
        await trans_seession_right.commit()
        trans_seession_right.__init__(bind=trans_seession_left.bind)


        sql_statement = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_right,'right','yes')
        print(sql_statement)
        result = await trans_seession_right.exec(text(sql_statement)) 
        hash_mismatch_all_cols_df_right = pd.DataFrame(result.fetchall())
        await publish_log(request=request,channels=channels,log_str="Hash mismatch all columns for right table calculated.",config_id=comparemodel.id,logtype="info")
        
    

    #transaction_remote.__init__(bind=transaction_remote.bind)

    #===============
    df_left_extra,df_right_extra = get_extra_rows_using_hash(hash_df_left, hash_df_right)
    ic(df_left_extra)
    ic(df_right_extra)


    mismatched_df = find_mismatched_rows(
        hash_df_left[["ConcatenatedKeys", "HashValue"]].copy(),
        hash_df_right[["ConcatenatedKeys", "HashValue"]].copy(),
    )
    ic(hash_mismatch_all_cols_df_left)
    mismatched_dict = compare_mismatched_rows(hash_mismatch_all_cols_df_left, hash_mismatch_all_cols_df_right, mismatched_df,df_left_extra,df_right_extra)
    ic(mismatched_dict)
    await publish_log(request=request,channels=channels,log_str="Mismatched rows calculated.",config_id=comparemodel.id,logtype="info")


    passed_col_df = pd.DataFrame(columns=['column_name', 'ConcatenatedKeys', 'value_df_left', 'value_df_right'])
    ic(passed_col_df)

    if len(diff_df) > 0:
        passed_table_sql_template = "sql/get_passed_table_rows.txt"
    else:
        passed_table_sql_template = "sql/get_passed_table_rows_success.txt"

    passed_sql_statement_left = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_left,'left','yes',passed_table_sql_template)
    result = await trans_seession_left.exec(text(passed_sql_statement_left)) 
    hash_passed_df_left = pd.DataFrame(result.fetchall())
    ic(hash_passed_df_left)
    if len(diff_df) > 0:
        await trans_seession_left.exec(text(left_sql_remove_table_for_hashstore))

    passed_sql_statement_right = await get_tbl_full_hash_sql(comparemodel, request, trans_seession_right,'right','yes',passed_table_sql_template)
    result = await trans_seession_right.exec(text(passed_sql_statement_right)) 
    hash_passed_df_right = pd.DataFrame(result.fetchall())
    ic(hash_passed_df_right)
    if len(diff_df) > 0:
        await trans_seession_right.exec(text(right_sql_remove_table_for_hashstore))
    
    common_df = get_common_rows(hash_passed_df_left, hash_passed_df_right, ["ConcatenatedKeys"])
    common_df.set_index('ConcatenatedKeys', inplace=False)
    ic(common_df)
    await publish_log(request=request,channels=channels,log_str="Common rows calculated.",config_id=comparemodel.id,logtype="info")

    passed_benedict_list : list[benedict] = []

    for column in hash_passed_df_left.columns:
        #ic(column)
        for index, common_row in common_df.head(10).iterrows():
                    # New row as a DataFrame
            if column in ['ConcatenatedKeys','HashValue']:
                continue
            item =  benedict()
            item.column_name = column
            item.ConcatenatedKeys = common_row.ConcatenatedKeys
            item.value_df_left = common_row[f'{column}_x']
            item.value_df_right = common_row[f'{column}_y']
            passed_benedict_list.append(item)
    passed_col_df = pd.DataFrame(passed_benedict_list)
    path = f'/home/deck/Downloads'
    file = f'{path}/{filename}.xlsx'
    with pd.ExcelWriter(file, engine="xlsxwriter") as writer:
        # Create a new dataframe with the column names from df_left
        summary_df = pd.DataFrame({"ColumnName": hash_passed_df_left.columns})

        # Write the dataframe to the Excel sheet
        summary_df["MatchedRowsCountLeft"] = summary_df["ColumnName"].apply(
            lambda col: len(hash_df_left)
            - len(df_left_extra)
            - ((len(mismatched_dict[col]) if col in mismatched_dict else 0) - (len(df_left_extra)+len(df_right_extra)))
        )

        summary_df["MatchedRowsCountRight"] = summary_df["ColumnName"].apply(
            lambda col: len(hash_df_right)
            - len(df_right_extra)
            - ((len(mismatched_dict[col]) if col in mismatched_dict else 0) - (len(df_left_extra)+len(df_right_extra)))
        )


        summary_df["MisMatchedRowsCount"] = summary_df["ColumnName"].apply(
            lambda col: (len(mismatched_dict[col]) if col in mismatched_dict else 0)
        )

        summary_df["ExtraInLeft"] = summary_df["ColumnName"].apply(
            lambda col: len(df_left_extra)
        )
        summary_df["ExtraInRight"] = summary_df["ColumnName"].apply(
            lambda col: len(df_right_extra)
        )

        summary_df["TotalRowsInLeft"] = len(hash_df_left)
        summary_df["TotalRowsInRight"] = len(hash_df_right)

        
        summary_df = summary_df.drop([0, 1])
        meta_df = pd.DataFrame(columns=['ConfigName', 'ConfigValue'])
        meta_df.loc[0] = ['LeftDatabaseName', static.SConstants.db_names[comparemodel.left_db]]
        meta_df.loc[1] = ['LeftTableName', comparemodel.left_tbl]
        meta_df.loc[2] = ['RightDatabaseName', static.SConstants.db_names[comparemodel.right_db]]
        meta_df.loc[3] = ['RightTableName', comparemodel.right_tbl]
        meta_df.loc[4] = ['UniqueColumns', comparemodel.unique_columns]
        meta_df.loc[5] = ['ExcludedColumns', comparemodel.columns_to_exclude]


        meta_df.to_excel(writer, sheet_name="MetaInfo", index=False)

        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        passed_col_df.to_excel(writer, sheet_name="PassRecordSummary", index=False)

        
        
        for column_name, differ_items in mismatched_dict.items():
            mismatched_df = pd.DataFrame(columns=['ConcatenatedKeys', 'value_df_left', 'value_df_right'])
            mismatched_bendict_list : list[benedict] = []
            for dit in differ_items:
                item =  benedict()
                item.ConcatenatedKeys = dit.ConcatenatedKeys
                item.value_df_left = dit.df_left_value
                item.value_df_right = dit.df_right_value
                mismatched_bendict_list.append(item)
            mismatched_df = pd.DataFrame(mismatched_bendict_list)

            ic(df_left_extra)
            mismatched_df.to_excel(writer, sheet_name=f"{column_name}_miss", index=False)
    await highlight_positive_cells(file,'Summary','MisMatchedRowsCount')
    await highlight_positive_cells(file,'Summary','ExtraInLeft')
    await highlight_positive_cells(file,'Summary','ExtraInRight')
    await highlight_cells_with_value(file,'Does_Not_Exists')
    await make_header_background_grey(file)
    await publish_log(request=request,channels=channels,log_str="Excel file created.",config_id=comparemodel.id,logtype="info")
    return f"{filename}.xlsx"

