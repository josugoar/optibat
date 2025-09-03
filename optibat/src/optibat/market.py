from datetime import datetime, timedelta
from importlib.resources import files
from typing import assert_never

import pandas as pd
import sqlalchemy
import streamlit as st
from box import Box
from pandas import DataFrame
from sqlalchemy import Engine
from sqlalchemy.engine import Connectable
from streamlit import runtime


def query_market(data: Box) -> Box:
    market_datetime = _to_datetime(data)
    market_input = _from_sql(market_datetime, data) if data.market_csv is None else _from_csv(data)  # fmt: off
    market = Box(
        market_datetime=market_datetime,
        market_input=market_input,
        **market_input,
    )
    return data | market


def _to_datetime(data: Box) -> datetime:
    match data.market_type:
        case "MD":
            market_datetime = data.market_date
            return market_datetime
        case "MI1":
            market_datetime = data.market_date
            return market_datetime
        case "MI2":
            market_datetime = data.market_date
            return market_datetime
        case "MI3":
            market_datetime = data.market_date + timedelta(days=1) - timedelta(hours=12)
            return market_datetime
        case "MIC":
            market_datetime = data.market_date + timedelta(hours=data.current_datetime.hour + 1) + timedelta(hours=1)  # fmt: off
            return market_datetime
        case _:
            assert_never()


def _from_sql(market_datetime: datetime, data: Box) -> DataFrame:
    sql = _read_sql_text()
    con = _connect(data)
    market = _query(sql, con, market_datetime, data)
    market = _index(market, data)
    return market


def _read_sql_text() -> str:
    sql = files("optibat").joinpath("sql/optibat.sql").read_text(encoding="utf-8")
    return sql


def _connect(data: Box) -> Engine:
    # fmt: off
    create_engine = (
        st.cache_resource(sqlalchemy.create_engine, show_spinner=False)
        if runtime.exists()
        else sqlalchemy.create_engine
    )
    con = create_engine(
        "oracle+oracledb://@",
        connect_args={"user": data.market.user, "password": data.market.password, "dsn": data.market.name},
        pool_pre_ping=True,
        thick_mode=True,
    )
    return con


def _query(sql: str, con: Connectable, market_datetime: datetime, data: Box) -> DataFrame:  # fmt: off
    market = pd.read_sql_query(
        sql,
        con,
        params={
            "market_datetime": market_datetime,
            "market_type": data.market_type,
            "market_horizon_day": data.market_horizon_day,
            "market_history_day": data.market_history_day,
            "market_forecast": data.market_forecast,
            "dim_ufi_bess_grid_import": data.dim_ufi_bess_grid_import,
            "dim_ufi_bess_grid_export": data.dim_ufi_bess_grid_export,
            "dim_ufi_res_grid_export": data.dim_ufi_res_grid_export,
            "dim_up_grid_export": data.dim_up_grid_export,
        },
    )
    return market


def _index(market: DataFrame, data: Box) -> DataFrame:
    # fmt: off
    market_dates = market.market_dates.dt.tz_localize(data.market_timezone)
    market_datetimes = market_dates + (market.market_periods - 1) * timedelta(minutes=data.market_time_unit_minute)
    initial_market_date = market_dates.iloc[0]
    final_market_date = market_dates.iloc[-1]
    days = pd.Series(data=(market_datetimes - initial_market_date).dt.days + 1, dtype=str)
    days = days.str.zfill(days.str.len().max())
    hours = pd.Series(data=market_datetimes.dt.hour + 1, dtype=str)
    hours = hours.str.zfill(hours.str.len().max())
    quarters = pd.Series(data=market_datetimes.dt.minute // data.market_time_unit_minute + 1, dtype=str)
    quarters = quarters.str.zfill(quarters.str.len().max())
    market = market.set_axis(
        ("D" + days if initial_market_date != final_market_date else "")
        + ("H" + hours)
        + ("Q" + quarters),
    )
    return market


def _from_csv(data: Box) -> DataFrame:
    data.market_csv.seek(0)
    market = pd.read_csv(
        data.market_csv,
        sep=";",
        index_col=0,
        parse_dates=["market_dates"],
        date_format="%d/%m/%Y",
    )
    return market
