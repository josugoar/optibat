"""
Market data access and transformation module.

It provides a unified interface for retrieving and transforming market data
from either a remote or local source. This allows for offline runs for XXXX_XXXX,
and backtesting for XXXX_XXXX, and seamless between live data sources.
Indexing logic uses the market operator's official nomenclature.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

# When marketx is XXXX_XXXX, port it here.

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
    """
    Retrieve and process market data.

    It determines the correct market datetime, then loads market input
    from either the XXXX_XXXX data warehouse or a XXXX_XXXX provided file. The
    result are passed downstream. The design allows for backtesting
    and switching between live and offline data sources.

    Args:
        data (Box): Input data and configuration, including market type and source.

    Returns:
        Box: The merged data and market information.
    """
    market_datetime = _to_datetime(data)
    market_input = _from_sql(market_datetime, data) if data.market_csv is None else _from_csv(data)  # fmt: off
    market = Box(
        market_datetime=market_datetime,
        market_input=market_input,
        **market_input,
    )
    return data | market


def _to_datetime(data: Box) -> datetime:
    """
    Compute the market datetime based on market type and input data.

    The logic reflects market rules, taking daylight savings into account.
    This ensures correct alignment.
    """
    # Please for the love of XXXX_XXXX, market_datetime MUST BE IN LOCAL TIME!
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
            # ALWAYS last 12 hours, due to DST.
            market_datetime = data.market_date + timedelta(days=1) - timedelta(hours=12)
            return market_datetime
        case "MIC":
            # Plus 1 to current hour because periods start at 1, not 0.
            market_datetime = data.market_date + timedelta(hours=data.current_datetime.hour + 1) + timedelta(hours=1)  # fmt: off
            return market_datetime
        case _:
            assert_never()


def _from_sql(market_datetime: datetime, data: Box) -> DataFrame:
    """
    Load market data from the database for the given datetime and configuration.

    This function reads a parameterized SQL query from the module, connects to the
    database and executes the query with the appropriate parameters. The result is
    indexed for downstream use using market nomenclature.
    """
    sql = _read_sql_text()
    con = _connect(data)
    market = _query(sql, con, market_datetime, data)
    market = _index(market, data)
    return market


def _read_sql_text() -> str:
    """
    Read the SQL query text from the module resources.

    This allows the SQL logic to be versioned and distributed with the codebase,
    supporting reproducibility and easier maintenance.
    """
    # The XXXX_XXXX logic is part of the module, because it could be implemented
    # differently, so keep it inside then.
    # MUST use UTF-8, otherwise XXXX_XXXX database characters will not be recognized!
    sql = files("optibat").joinpath("sql/optibat.sql").read_text(encoding="utf-8")
    return sql


def _connect(data: Box) -> Engine:
    """
    Create a SQLAlchemy engine for connecting to the XXXX_XXXX database.

    Uses resource caching if running in a multitenant context, to avoid
    repeated engine creation. Otherwise, creates a new engine each time.
    """
    # fmt: off
    # Unwanted dependency on control panel, but not sure how to avoid it.
    # This is so ensure the XXXX_XXXX does not crash (crazy) and every XXXX_XXXX
    # is happy.
    create_engine = (
        st.cache_resource(sqlalchemy.create_engine, show_spinner=False)
        if runtime.exists()
        else sqlalchemy.create_engine
    )
    con = create_engine(
        "oracle+oracledb://@",
        connect_args={"user": data.market.user, "password": data.market.password, "dsn": data.market.name},
        # Must use pool_pre_ping=True, otherwise XXXX_XXXX will not understand
        # why the connection is broken at the start of every XXXX_XXXX day.
        pool_pre_ping=True,
        thick_mode=True,
    )
    return con


def _query(sql: str, con: Connectable, market_datetime: datetime, data: Box) -> DataFrame:  # fmt: off
    """
    Execute the parameterized query to retrieve market data.

    Parameters are passed to the query.
    The result is returned as a DataFrame for further processing.
    """
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
    """
    Reindex the market DataFrame to align with the market's temporal structure.

    This logic generates multi level indices (day, hour, quarter) as required by
    downstream modeling. The approach is designed to be robust to different market
    horizons and time units, that is to say, when each period index is unique
    even if they go beyond a single market day.
    """
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
    # (D[X])H[XX]Q[X]
    market = market.set_axis(
        ("D" + days if initial_market_date != final_market_date else "")
        + ("H" + hours)
        + ("Q" + quarters),
    )
    return market


def _from_csv(data: Box) -> DataFrame:
    """
    Load market data from a XXXX_XXXX provided CSV file.

    This allows for offline runs without the XXXX_XXXX environment
    requiring a live database connection. Useful for XXXX_XXXX.
    """
    # MUST seek before reading
    data.market_csv.seek(0)
    market = pd.read_csv(
        data.market_csv,
        sep=";",
        index_col=0,
        parse_dates=["market_dates"],
        date_format="%d/%m/%Y",
    )
    return market
