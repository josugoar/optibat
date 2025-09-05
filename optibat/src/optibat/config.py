"""
Configuration management module.

It loads, validates and adapts configuration for the optimization pipeline.
It uses Dynaconf for layered configuration, supporting environment variables, user secrets,
and project-local files. Validators ensure that all required settings are present and have
reasonable defaults, so the rest of the system can assume valid configuration at runtime.

Hooks are provided to dynamically adapt configuration values (such as current datetime and market date)
based on runtime data.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

import math
from datetime import date, datetime, time, timedelta
from typing import assert_never
from zoneinfo import ZoneInfo

from box import Box
from dynaconf import Dynaconf, Validator

# DO NOT add typing for the settings. There is an opened pull request XXXX_XXXX
# and issue XXXX_XXXX for that, BUT it was a failure and nobody cares.
settings = Dynaconf(
    envvar_prefix="OPTIBAT",
    # Must load user wide settings first and config before secrets! Otherwise
    # XXXX_XXXX will override the settings and XXXX_XXXX WILL BREAK.
    settings_files=[
        "~/.optibat/config.yaml",
        "~/.optibat/.secrets.yaml",
        ".optibat/config.yaml",
        ".optibat/.secrets.yaml",
    ],
    environments=True,
    load_dotenv=True,
    env_switcher="OPTIBAT_ENV",
    env="default",
)

# Default values are given by XXXX_XXXX recommendations.
settings.validators.register(
    Validator(
        "HEADLESS",
        default=False,
        is_type_of=bool,
    ),
    Validator(
        "CURRENT_DATETIME",
        default=None,
        is_type_of=datetime | None,
    ),
    Validator(
        "MARKET_DATE",
        default=None,
        is_type_of=date | None,
    ),
    Validator(
        "MARKET_TIMEZONE",
        default="Europe/Madrid",
        is_type_of=str,
    ),
    Validator(
        "MARKET_TYPE",
        default="MD",
        # Add other frr markets when needed.
        is_in=["MD", "MI1", "MI2", "MI3", "MIC"],
    ),
    Validator(
        "MARKET_HORIZON_DAY",
        default=7,
        is_type_of=int,
        gte=0,
    ),
    Validator(
        "MARKET_HISTORY_DAY",
        default=31,
        is_type_of=int,
        gte=0,
    ),
    Validator(
        "MARKET_FORECAST",
        default="XXXX_XXXX",
        is_in=["XXXX_XXXX", "XXXX_XXXX"],
    ),
    Validator(
        "MARKET_RATE",
        default=0.001,
        is_type_of=float,
    ),
    Validator(
        "MARKET_TIME_UNIT_MINUTE",
        default=15,
        is_type_of=int,
        gte=0,
    ),
    Validator(
        "DIM_UFI_BESS_GRID_IMPORT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_UFI_BESS_RES_IMPORT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_UFI_BESS_GRID_EXPORT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_UFI_BESS_CHARGE",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_UFI_RES_GRID_EXPORT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_UP_GRID_EXPORT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_STATE_OF_CHARGE_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_STATE_OF_HEALTH_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_CHARGING_POWER_CAPACITY_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_DISCHARGING_POWER_CAPACITY_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_AVAILABILITY_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_CHARGING_EFFICIENCY_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_DISCHARGING_EFFICIENCY_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "DIM_PROGRAM_POINT",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "BESS_POWER_CAPACITY_MEGAWATT",
        default=5.0,
        is_type_of=float,
        gte=0.0,
    ),
    Validator(
        "BESS_ENERGY_CAPACITY_MEGAWATT_HOUR",
        default=5.0,
        is_type_of=float,
        gte=0.0,
    ),
    Validator(
        "BESS_CHARGING_EFFICIENCY_PERCENT",
        default=100.0,
        is_type_of=float,
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_DISCHARGING_EFFICIENCY_PERCENT",
        default=100.0,
        is_type_of=float,
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_MAXIMUM_CYCLES_COUNT_PER_DAY",
        default=1.0,
        is_type_of=float,
        gte=0.0,
    ),
    Validator(
        "BESS_PROFIT_THRESHOLD_EURO_PER_MEGAWATT_HOUR",
        default=60.0,
        is_type_of=float,
        gte=0.0,
    ),
    Validator(
        "BESS_MINIMUM_STATE_OF_CHARGE_PERCENT",
        default=0.0,
        is_type_of=float,
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_MAXIMUM_STATE_OF_CHARGE_PERCENT",
        default=100.0,
        is_type_of=float,
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_INITIAL_STATE_OF_CHARGE_PERCENT",
        default=None,
        is_type_of=float | None,
    ),
    Validator(
        "BESS_INITIAL_STATE_OF_CHARGE_PERCENT",
        when=Validator("BESS_INITIAL_STATE_OF_CHARGE_PERCENT", is_type_of=float),
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_FINAL_STATE_OF_CHARGE_PERCENT",
        default=None,
        is_type_of=float | None,
    ),
    Validator(
        "BESS_FINAL_STATE_OF_CHARGE_PERCENT",
        when=Validator("BESS_FINAL_STATE_OF_CHARGE_PERCENT", is_type_of=float),
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_AVAILABILITY_PERCENT",
        default=None,
        is_type_of=float | None,
    ),
    Validator(
        "BESS_AVAILABILITY_PERCENT",
        when=Validator("BESS_AVAILABILITY_PERCENT", is_type_of=float),
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_RES_IMPORT_CLIPPING_PERCENT",
        default=100.0,
        is_type_of=float,
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_RES_IMPORT_CLIPPING_THRESHOLD_MEGAWATT",
        default=0.0,
        is_type_of=float,
        gte=0.0,
    ),
    Validator(
        "BESS_RES_IMPORT_PRIORITY",
        default=False,
        is_type_of=bool,
    ),
    Validator(
        "BESS_STATE_OF_CHARGE_TOLERANCE_PERCENT",
        default=0.0,
        is_type_of=float,
        gte=0.0,
        lte=100.0,
    ),
    Validator(
        "BESS_PURCHASE_TOLERANCE_EURO_PER_MEGAWATT_HOUR",
        default=5.0,
        is_type_of=float,
    ),
    Validator(
        "BESS_SALE_TOLERANCE_EURO_PER_MEGAWATT_HOUR",
        default=5.0,
        is_type_of=float,
    ),
    Validator(
        "BESS_GRID_IMPORT_NET_FIXED_MEGAWATT",
        default=lambda settings, validator: dict(),
        is_type_of=dict,
    ),
    Validator(
        "BESS_RES_IMPORT_FIXED_MEGAWATT",
        default=lambda settings, validator: dict(),
        is_type_of=dict,
    ),
    Validator(
        "BESS_GRID_EXPORT_NET_FIXED_MEGAWATT",
        default=lambda settings, validator: dict(),
        is_type_of=dict,
    ),
    Validator(
        "BESS_STATE_OF_CHARGE_FIXED_PERCENT",
        default=lambda settings, validator: dict(),
        is_type_of=dict,
    ),
    Validator(
        "RES_EXPORT_PRICE_EURO_PER_MEGAWATT_HOUR",
        default=None,
        is_type_of=float | None,
    ),
    Validator(
        "GRID_EXPORT_LIMIT_MEGAWATT",
        default=math.inf,
        is_type_of=float,
    ),
    Validator(
        "SOLVER",
        default="glpk",
        is_type_of=str,
    ),
    Validator(
        "OUTPUT_XXXX_XXXX_PATH",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "OUTPUT_XXXX_XXXX_PATH",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "OUTPUT_CSV_PATH",
        default=None,
        is_type_of=str | None,
    ),
    Validator(
        "OUTPUT_BLOCK",
        default=1,
        is_type_of=int,
    ),
    Validator(
        "AUTO_ENABLED",
        default=True,
        is_type_of=bool,
    ),
    Validator(
        "MODULES",
        default=lambda settings, validator: list(),
        is_type_of=list,
    ),
    # Secrets:
    Validator(
        "AUTH",
        default=None,
        is_type_of=dict | None,
    ),
    Validator(
        "AUTH.NAME",
        must_exist=True,
        when=Validator("AUTH", is_type_of=dict),
        is_type_of=str,
    ),
    Validator(
        "MARKET",
        default=None,
        is_type_of=dict | None,
    ),
    Validator(
        "MARKET.USER",
        must_exist=True,
        when=Validator("MARKET", is_type_of=dict),
        is_type_of=str,
    ),
    Validator(
        "MARKET.PASSWORD",
        must_exist=True,
        when=Validator("MARKET", is_type_of=dict),
        is_type_of=str,
    ),
    Validator(
        "MARKET.NAME",
        must_exist=True,
        when=Validator("MARKET", is_type_of=dict),
        is_type_of=str,
    ),
    Validator(
        "METERING",
        default=None,
        is_type_of=dict | None,
    ),
    Validator(
        "METERING.USER",
        must_exist=True,
        when=Validator("METERING", is_type_of=dict),
        is_type_of=str,
    ),
    Validator(
        "METERING.PASSWORD",
        must_exist=True,
        when=Validator("METERING", is_type_of=dict),
        is_type_of=str,
    ),
    Validator(
        "METERING.NAME",
        must_exist=True,
        when=Validator("METERING", is_type_of=dict),
        is_type_of=str,
    ),
)

settings.validators.validate_all()


def update_config(data: Box) -> Box:
    """
    Update configuration with runtime values for current datetime and market date.

    This function injects context-sensitive values into the configuration, allowing the
    optimization pipeline to operate with the correct temporal context. This is essential
    because settings MUST NOT depend on runtime data that changes during subsequent runs.

    Args:
        data (Box): Configuration data, possibly containing overrides for datetime or market date.

    Returns:
        Box: The merged data and settings with hooks applied.
    """
    current_datetime = _current_datetime_hook(data)
    market_date = _market_date_hook(data)
    settings = Box(current_datetime=current_datetime, market_date=market_date)
    return data | settings


def _current_datetime_hook(data: Box) -> datetime:
    """
    Resolve the current datetime, using overrides if provided.

    This allows the pipeline to be run multiple times for different points in time,
    which is useful for backtesting, simulation and XXXX_XXXX.
    """
    current_datetime = (
        data.current_datetime.replace(tzinfo=ZoneInfo(data.market_timezone))
        if data.current_datetime is not None
        else _default_current_datetime_hook(data)
    )
    return current_datetime


def _default_current_datetime_hook(data: Box) -> datetime:
    """
    Return the current system datetime in the configured market timezone.
    """
    current_datetime = datetime.now(tz=ZoneInfo(data.market_timezone))
    return current_datetime


def _market_date_hook(data: Box) -> datetime:
    """
    Resolve the market date for the pipeline, using overrides if provided.

    This enables the pipeline to be run for different market days, supporting
    both live and historical analysis.
    """
    market_date = datetime.combine(
        data.market_date
        if data.market_date is not None
        else _default_market_date_hook(data),
        time.min,
        tzinfo=ZoneInfo(data.market_timezone),
    )
    return market_date


def _default_market_date_hook(data: Box) -> date:
    """
    Compute the default market date based on the market type.

    The logic reflects market rules: for some market types, the relevant date is tomorrow,
    for others, it is today. This ensures the workflow always uses a valid market date
    even if not explicitly provided.
    """
    # https://www.omie.es/es/mercado-de-electricidad BUT TAKE TIMEZONES INTO ACCOUNT!
    match data.market_type:
        case "MD":
            market_date = date.today() + timedelta(days=1)
            return market_date
        case "MI1":
            market_date = date.today() + timedelta(days=1)
            return market_date
        case "MI2":
            market_date = date.today() + timedelta(days=1)
            return market_date
        case "MI3":
            market_date = date.today()
            return market_date
        case "MIC":
            market_date = date.today()
            return market_date
        case _:
            assert_never()
