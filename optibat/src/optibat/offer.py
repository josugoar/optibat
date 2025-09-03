from __future__ import annotations

import numpy as np
import pandas as pd
from box import Box
from pandas import Series


def quote_price(data: Box) -> Box:
    bess_price_euro_per_megawatt_hour = _quote_bess_price(data)
    res_price_euro_per_megawatt_hour = _quote_res_price(data)
    offer = Box(
        bess_price_euro_per_megawatt_hour=bess_price_euro_per_megawatt_hour,
        res_price_euro_per_megawatt_hour=res_price_euro_per_megawatt_hour,
    )
    return data | offer


def _quote_bess_price(data: Box) -> Series[float]:
    bess_state_of_charge_diff_megawatt_hour = (
        data.bess_state_of_charge_megawatt_hour
        - data.bess_previous_state_of_charge_megawatt_hour
    )

    bess_state_of_charge_signed_diff = np.sign(bess_state_of_charge_diff_megawatt_hour)

    # fmt: off
    bess_state_of_charge_lower_bound = (
        data.bess_state_of_charge_megawatt_hour
        <= (data.bess_minimum_state_of_charge_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
        + (data.bess_state_of_charge_tolerance_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
    )
    # fmt: on

    bess_initial_state_of_charge_lower_bound = (
        bess_state_of_charge_lower_bound
        & ~bess_state_of_charge_lower_bound.shift(fill_value=False)
    )

    # fmt: off
    bess_state_of_charge_upper_bound = (
        data.bess_state_of_charge_megawatt_hour
        >= min(data.bess_maximum_state_of_charge_percent / 100.0, data.bess_availability_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
        - (data.bess_state_of_charge_tolerance_percent / 100.0)
        * data.bess_energy_capacity_megawatt_hour
    )
    # fmt: on

    bess_initial_state_of_charge_upper_bound = (
        bess_state_of_charge_upper_bound
        & ~bess_state_of_charge_upper_bound.shift(fill_value=False)
    )

    bess_state_of_charge_bound = (
        bess_initial_state_of_charge_lower_bound
        | bess_initial_state_of_charge_upper_bound
    )

    bess_initial_state_of_charge_bound = bess_state_of_charge_bound.shift(
        fill_value=False,
    )

    bess_cycles_count = bess_initial_state_of_charge_bound.cumsum() + 1

    bess_signed_cycles = bess_state_of_charge_signed_diff * bess_cycles_count

    market_price_euro_per_megawatt_hour_by_bess_signed_cycles_groups = (
        data.market_price_euro_per_megawatt_hour.groupby(by=bess_signed_cycles)
    )

    # fmt: off
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
    # fmt: on

    bess_price_euro_per_megawatt_hour = bess_price_euro_per_megawatt_hour.where(
        data.market_price_euro_per_megawatt_hour.notna(),
    )

    return bess_price_euro_per_megawatt_hour


def _quote_res_price(data: Box) -> Series[float]:
    res_export_price_euro_per_megawatt_hour = pd.Series(
        data=data.res_export_price_euro_per_megawatt_hour,
        index=data.market_input.index,
        dtype=float,
    )

    res_price_euro_per_megawatt_hour = res_export_price_euro_per_megawatt_hour.where(
        data.market_price_euro_per_megawatt_hour.notna(),
    )

    return res_price_euro_per_megawatt_hour
