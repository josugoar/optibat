"""
Offer price calculation for bids.

It provides logic to generate price offers for battery (BESS) and renewable energy sources (RES)
based on charging semicycle logic. The design ensures that offers reflect both market context and
system constraints, supporting adaptive bidding. Mainly, that all bids are competitive.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from box import Box
from pandas import Series


def quote_price(data: Box) -> Box:
    """
    Generate price offers for battery and renewable energy sources.

    This function computes the offer prices for both the battery energy storage system (BESS)
    and renewable export (RES). Prices are only set for market horizon.

    Args:
        data (Box): Input data and configuration, including market and module state.

    Returns:
        Box: The merged data and offer price information.
    """
    bess_price_euro_per_megawatt_hour = _quote_bess_price(data)
    res_price_euro_per_megawatt_hour = _quote_res_price(data)
    offer = Box(
        bess_price_euro_per_megawatt_hour=bess_price_euro_per_megawatt_hour,
        res_price_euro_per_megawatt_hour=res_price_euro_per_megawatt_hour,
    )
    return data | offer


def _quote_bess_price(data: Box) -> Series[float]:
    """
    Calculate the BESS offer price series based on state of charge and market context.

    The logic tracks charge and discharge cycles, applies upper and lower state of charge bounds,
    and adjusts prices using configured tolerances. This approach ensures that offers are
    competitive while respecting system constraints and operational safety. Offers are based on
    charging semicycle logic, so the price is adjusted so that each group is competitively
    matched individually. See XXXX_XXXX for details.
    """
    # Track the change in state of charge to identify charge/discharge cycles.
    bess_state_of_charge_diff_megawatt_hour = (
        data.bess_state_of_charge_megawatt_hour
        - data.bess_previous_state_of_charge_megawatt_hour
    )

    bess_state_of_charge_signed_diff = np.sign(bess_state_of_charge_diff_megawatt_hour)

    # Detect when the state of charge hits lower bounds, considering tolerances.
    bess_state_of_charge_lower_bound = (
        data.bess_state_of_charge_megawatt_hour
        <= (data.bess_minimum_state_of_charge_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
        + (data.bess_state_of_charge_tolerance_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
    )

    # Identify the start of each new discharge cycle heterogeneously.
    bess_initial_state_of_charge_lower_bound = (
        bess_state_of_charge_lower_bound
        & ~bess_state_of_charge_lower_bound.shift(fill_value=False)
    )

    # Detect when the state of charge hits upper bounds, considering tolerances AND availability
    # AND health. If a BESS breaks because of XXXX_XXXX, it will still work and use the
    # remaining capacity.
    bess_state_of_charge_upper_bound = (
        data.bess_state_of_charge_megawatt_hour
        >= min(data.bess_maximum_state_of_charge_percent / 100.0, data.bess_availability_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
        - (data.bess_state_of_charge_tolerance_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
    )

    # Identify the start of each new charge cycle heterogeneously.
    bess_initial_state_of_charge_upper_bound = (
        bess_state_of_charge_upper_bound
        & ~bess_state_of_charge_upper_bound.shift(fill_value=False)
    )

    # Identify the start of each new charge and discharge cycle homogeneously.
    bess_state_of_charge_bound = (
        bess_initial_state_of_charge_lower_bound
        | bess_initial_state_of_charge_upper_bound
    )

    # Fix off by one error? This work but there is no clear indication of why in XXXX_XXXX.
    bess_initial_state_of_charge_bound = bess_state_of_charge_bound.shift(
        fill_value=False,
    )

    # Count cycles to group market periods by charge and discharge events. Each semicycle
    # gets its own unique identifier.
    bess_cycles_count = bess_initial_state_of_charge_bound.cumsum() + 1
    bess_signed_cycles = bess_state_of_charge_signed_diff * bess_cycles_count

    # Group market prices by charge/discharge cycles and apply price logic.
    market_price_euro_per_megawatt_hour_by_bess_signed_cycles_groups = (
        data.market_price_euro_per_megawatt_hour.groupby(by=bess_signed_cycles)
    )
    bess_price_euro_per_megawatt_hour = market_price_euro_per_megawatt_hour_by_bess_signed_cycles_groups.transform(
        lambda market_price_euro_per_megawatt_hour_by_bess_signed_cycles_group: (
            market_price_euro_per_megawatt_hour_by_bess_signed_cycles_group.min()
            - data.bess_sale_tolerance_euro_per_megawatt_hour
            if market_price_euro_per_megawatt_hour_by_bess_signed_cycles_group.name < 0.0
            else market_price_euro_per_megawatt_hour_by_bess_signed_cycles_group.max()
            + data.bess_purchase_tolerance_euro_per_megawatt_hour
            if market_price_euro_per_megawatt_hour_by_bess_signed_cycles_group.name > 0.0
            else 0.0
        ),
    )

    bess_price_euro_per_megawatt_hour = bess_price_euro_per_megawatt_hour.where(
        data.market_price_euro_per_megawatt_hour.notna(),
    )

    # Charge -> positive
    # Discharge -> negative
    # Steady state -> 0
    # Id per semicycle

    return bess_price_euro_per_megawatt_hour


def _quote_res_price(data: Box) -> Series[float]:
    """
    Calculate the renewable export offer price series.

    This function simply propagates the configured export price to all market periods
    where a market price is available. This supports scenarios where the renewable
    export price is fixed or externally determined.
    """
    # RES price is manual because of bilateral XXXX_XXXX agreements. And it should
    # ALWAYS match anyway, unless negative prices? Consider XXXX_XXXX.
    res_export_price_euro_per_megawatt_hour = pd.Series(
        data=data.res_export_price_euro_per_megawatt_hour,
        index=data.market_input.index,
        dtype=float,
    )

    res_price_euro_per_megawatt_hour = res_export_price_euro_per_megawatt_hour.where(
        data.market_price_euro_per_megawatt_hour.notna(),
    )

    return res_price_euro_per_megawatt_hour
