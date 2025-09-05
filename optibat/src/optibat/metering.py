"""
Metering data access and transformation.

This module provides a unified interface for retrieving and transforming metering data
from either configuration or a PI System historian. It ensures that all required values
are available and normalized, filling with reasonable defaults when necessary. This approach
supports robust downstream modeling and simplifies integration with both real and simulated data sources,
because it does not requiere the module to be connected to the larger system for simulations.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

# PREPARE FOR XXXX_XXXX MIGRATION! BECAUSE THE XXXX_XXXX DOES NOT ALLOW ACCESS...

from __future__ import annotations

import typing
from datetime import timedelta

import pandas as pd
from box import Box
from pandas import DataFrame, Series

if typing.TYPE_CHECKING:
    from PIconnect import PIServer


def read_module(data: Box) -> Box:
    """
    Retrieve and process metering data.

    If no metering configuration is provided, fallback to configuration or default values.
    Otherwise, connect to the PI System historian and retrieve the required values.

    Args:
        data (Box): Input data and configuration, including metering source and overrides.

    Returns:
        Box: The merged data and metering information.
    """
    if data.metering is None:
        # In the future maybe move default values elsewhere?
        bess_initial_state_of_charge_percent = data.bess_initial_state_of_charge_percent if data.bess_initial_state_of_charge_percent is not None else data.bess_minimum_state_of_charge_percent  # fmt: off
        bess_state_of_health_percent = data.bess_state_of_health_percent if data.bess_state_of_health_percent is not None else 100.0  # fmt: off
        bess_charging_power_capacity_percent, bess_discharging_power_capacity = data.bess_power_capacity_percent if data.bess_power_capacity_percent is not None else (0.0, 0.0)  # fmt: off
        bess_availability_percent = data.bess_availability_percent if data.bess_availability_percent is not None else 100.0  # fmt: off
        bess_charging_efficiency_percent, bess_discharging_efficiency_percent = data.bess_efficiency_percent if data.bess_efficiency_percent is not None else (100.0, 100.0)  # fmt: off
        bess_actual_state_of_charge_megawatt_hour = pd.Series(data=None, index=data.market_input.index, dtype=float)  # fmt: off
        module = Box(
            bess_initial_state_of_charge_percent=bess_initial_state_of_charge_percent,
            bess_state_of_health_percent=bess_state_of_health_percent,
            bess_charging_power_capacity_percent=bess_charging_power_capacity_percent,
            bess_discharging_power_capacity=bess_discharging_power_capacity,
            bess_availability_percent=bess_availability_percent,
            bess_charging_efficiency_percent=bess_charging_efficiency_percent,
            bess_discharging_efficiency_percent=bess_discharging_efficiency_percent,
            bess_actual_state_of_charge_megawatt_hour=bess_actual_state_of_charge_megawatt_hour,
        )
        return data | module

    # Lazy load Piconnect, because there is an issue with the XXXX_XXXX
    # configuration of the server (or is it the library? https://github.com/Hugovdberg/PIconnect/issues/75).
    # Also use PI.AF direct Python.Net interface https://docs.aveva.com/category/pi-system
    import PIconnect as PI

    with PI.PIServer(
        server=data.metering.name,
        username=data.metering.user,
        password=data.metering.password,
    ) as server:
        bess_initial_state_of_charge_percent = _read_bess_initial_state_of_charge(server, data) if data.bess_initial_state_of_charge_percent is None else data.bess_initial_state_of_charge_percent  # fmt: off
        bess_state_of_health_percent = _read_bess_state_of_health(server, data) if data.bess_state_of_health_percent is None else data.bess_state_of_health_percent  # fmt: off
        bess_charging_power_capacity_percent, bess_discharging_power_capacity_percent = _read_bess_power_capacity(server, data) if data.bess_power_capacity_percent is None else data.bess_power_capacity_percent  # fmt: off
        bess_availability_percent = _read_bess_availability(server, data) if data.bess_availability_percent is None else data.bess_availability_percent  # fmt: off
        bess_charging_efficiency_percent, bess_discharging_efficiency_percent = _read_bess_efficiency(server, data) if data.bess_efficiency_percent is None else data.bess_efficiency_percent  # fmt: off
        bess_actual_state_of_charge_megawatt_hour = _read_bess_actual_state_of_charge(server, data)  # fmt: off
        module = Box(
            bess_initial_state_of_charge_percent=bess_initial_state_of_charge_percent,
            bess_state_of_health_percent=bess_state_of_health_percent,
            bess_charging_power_capacity_percent=bess_charging_power_capacity_percent,
            bess_discharging_power_capacity=bess_discharging_power_capacity_percent,
            bess_availability_percent=bess_availability_percent,
            bess_charging_efficiency_percent=bess_charging_efficiency_percent,
            bess_discharging_efficiency_percent=bess_discharging_efficiency_percent,
            bess_actual_state_of_charge_megawatt_hour=bess_actual_state_of_charge_megawatt_hour,
        )
        return data | module


def _read_bess_initial_state_of_charge(server: PIServer, data: Box) -> float:
    """
    Read the initial state of charge from the PI System historian, or fallback to minimum if unavailable.
    Tries twice to get a good value, as PI System data can be delayed or have quality issues.
    """
    from PIconnect.PIConsts import RetrievalMode

    # Couple retries because SoC returns incorrect values sometimes?
    for _ in range(2):
        dim_state_of_charge_point = server.search(data.dim_state_of_charge_point)
        (dim_state_of_charge_point,) = dim_state_of_charge_point

        bess_initial_state_of_charge_percent = dim_state_of_charge_point.recorded_value(
            time=data.market_datetime,
            # PLEASE USE AT_OR_BEFORE. Even though the documentation for the retrieval mode says
            # that it supposedly uses the last recorded value, signals are not synced
            # and the bess might charge or discharge incorrectly! It already costed XXXX_XXXX.
            retrieval_mode=RetrievalMode.AT_OR_BEFORE,
        )

        # If IEC 104 SoC signal quality is not checked, it may return values above the threshold.
        if (value.Status & PI.AF.Asset.AFValueStatus.QualityMask) != PI.AF.Asset.AFValueStatus.Good:
            continue

        bess_initial_state_of_charge_percent = pd.to_numeric(
            bess_initial_state_of_charge_percent,
            errors="coerce",
        )

        bess_initial_state_of_charge_percent = bess_initial_state_of_charge_percent.fillna(
            value=data.bess_minimum_state_of_charge_percent,
        )

        bess_initial_state_of_charge_percent = bess_initial_state_of_charge_percent.item()

        return bess_initial_state_of_charge_percent

    return 0.0


def _read_bess_state_of_health(server: PIServer, data: Box) -> float:
    """
    Read the state of health from the PI System historian, or fallback to default if unavailable.
    """
    from PIconnect.PIConsts import RetrievalMode

    dim_state_of_health_point = server.search(data.dim_state_of_health_point)
    (dim_state_of_health_point,) = dim_state_of_health_point

    bess_state_of_health_percent = dim_state_of_health_point.recorded_value(
        time=data.market_datetime,
        retrieval_mode=RetrievalMode.AT_OR_BEFORE,
    )

    bess_state_of_health_percent = pd.to_numeric(
        bess_state_of_health_percent,
        errors="coerce",
    )

    bess_state_of_health_percent = bess_state_of_health_percent.fillna(value=100.0)

    bess_state_of_health_percent = bess_state_of_health_percent.item()

    return bess_state_of_health_percent


def _read_bess_power_capacity(server: PIServer, data: Box) -> tuple[float, float]:
    """
    Read the charging and discharging power capacity from the PI System historian, or fallback to default if unavailable.
    Returns a tuple for (charging_power_capacity, discharging_power_capacity).
    """
    from PIconnect.PIConsts import RetrievalMode

    dim_power_capacity_points = server.search([data.dim_charging_power_capacity_point, data.dim_discharging_power_capacity_point])

    bess_power_capacity_percent = []

    for dim_power_capacity_point in dim_power_capacity_points:
        bess_power_capacity_percent = dim_power_capacity_point.recorded_value(
            time=data.market_datetime,
            retrieval_mode=RetrievalMode.AT_OR_BEFORE,
        )

        bess_power_capacity_percent = pd.to_numeric(
            bess_power_capacity_percent,
            errors="coerce",
        )

        bess_power_capacity_percent = bess_power_capacity_percent.fillna(value=0.0)

        bess_power_capacity_percent = bess_power_capacity_percent.item()

        bess_power_capacity_percents.append(bess_power_capacity_percent)

    return tuple(bess_power_capacity_percents)


def _read_bess_availability(server: PIServer, data: Box) -> float:
    """
    Read the availability from the PI System historian, or fallback to default if unavailable.
    """
    from PIconnect.PIConsts import RetrievalMode

    dim_availability_point = server.search(data.dim_availability_point)
    (dim_availability_point,) = dim_availability_point

    bess_availability_percent = dim_availability_point.recorded_value(
        time=data.market_datetime,
        retrieval_mode=RetrievalMode.AT_OR_BEFORE,
    )

    bess_availability_percent = pd.to_numeric(
        bess_availability_percent,
        errors="coerce",
    )

    bess_availability_percent = bess_availability_percent.fillna(value=100.0)

    bess_availability_percent = bess_availability_percent.item()

    return bess_availability_percent


def _read_bess_efficiency(server: PIServer, data: Box) -> tuple[float, float]:
    """
    Read the charging and discharging efficiency from the PI System historian, or fallback to default if unavailable.
    Returns a tuple for (charging_efficiency, discharging_efficiency). If no values are found, both are set to default.
    """
    from PIconnect.PIConsts import RetrievalMode

    dim_efficiency_points = server.search([data.dim_charging_efficiency_point, data.dim_discharging_efficiency_point])

    bess_efficiency_percent = []

    for dim_efficiency_point in dim_efficiency_points:
        bess_efficiency_percent = dim_efficiency_point.recorded_value(
            time=data.market_datetime,
            retrieval_mode=RetrievalMode.AT_OR_BEFORE,
        )

        bess_efficiency_percent = pd.to_numeric(
            bess_efficiency_percent,
            errors="coerce",
        )

        bess_efficiency_percent = bess_efficiency_percent.fillna(value=100.0)

        bess_efficiency_percent = bess_efficiency_percent.item()
        if pd.isna(bess_efficiency_percent):
            continue

        bess_efficiency_percents.append(bess_efficiency_percent)

    # Sometimes values are na because the IEC 104 general interrogations has not happend
    # (check of that and modify timers for getting to the RTU?)

    if len(bess_efficiency_percents) == 0:
        bess_efficiency_percents.append(100.0)
        bess_efficiency_percents.append(100.0)

    if len(bess_efficiency_percents) == 1:
        bess_efficiency_percents.append(bess_efficiency_percents[0])

    return tuple(bess_efficiency_percents)


def _read_bess_actual_state_of_charge(server: PIServer, data: Box) -> Series[float]:
    """
    Read the actual state of charge time series from the PI System historian, interpolated over the market horizon.
    Values are normalized to MWh using the configured energy capacity.
    """
    dim_state_of_charge_point = server.search(data.dim_state_of_charge_point)
    (dim_state_of_charge_point,) = dim_state_of_charge_point

    bess_actual_state_of_charge_megawatt_hour = (
        dim_state_of_charge_point.interpolated_values(
            data.market_date,
            data.market_date
            + timedelta(days=data.market_horizon_day)
            - timedelta(minutes=data.market_time_unit_minute),
            f"{data.market_time_unit_minute} minute",
        )
    )

    bess_actual_state_of_charge_megawatt_hour = pd.to_numeric(
        bess_actual_state_of_charge_megawatt_hour,
        errors="coerce",
    )

    bess_actual_state_of_charge_megawatt_hour = (
        bess_actual_state_of_charge_megawatt_hour.set_axis(data.market_input.index)
    )

    # MWh is needed, not %.
    bess_actual_state_of_charge_megawatt_hour = (
        bess_actual_state_of_charge_megawatt_hour / 100.0
    ) * data.bess_energy_capacity_megawatt_hour

    return bess_actual_state_of_charge_megawatt_hour


def write_module(data: Box, output_module: DataFrame) -> None:
    """
    Write the output module data back to the PI System historian.
    Only updates values that have changed, to minimize unnecessary writes.
    """
    import PIconnect as PI
    from PIconnect.PIConsts import UpdateMode, BufferMode

    with PI.PIServer(
        server=data.metering.name,
        username=data.metering.user,
        password=data.metering.password,
    ) as server:
        dim_program_point = server.search(data.dim_program_point)
        (dim_program_point,) = dim_program_point

        # Only use changes for the power profile, sending setpoints all the time is not advised
        output_module["ENERGIA"] = output_module["ENERGIA"].where(output_module["ENERGIA"] != output_module["ENERGIA"].shift())
        output_module["POTENCIA"] = output_module["POTENCIA"].where(output_module["POTENCIA"] != output_module["POTENCIA"].shift())

        for output in output_module.to_dict(orient="records"):
            if pd.isna(output["POTENCIA"]):
                continue

            point.update_value(
                output["POTENCIA"],
                # Future (or past technically if doing backtesting but it does not matter).
                PI.AF.Time.AFTime(output["FECHA"] + output["PERIODO"] * timedelta(minutes=data.market_time_unit_minute)),
                # ALWAYS replace because XXXX_XXXX and XXXX_XXXX might not update.
                UpdateMode.REPLACE,
                # And do not buffer for that matter, same reason. Speed is not relevant here.
                BufferMode.DO_NOT_BUFFER,
            )
