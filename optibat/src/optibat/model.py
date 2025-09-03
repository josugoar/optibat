from __future__ import annotations

import math
from contextlib import contextmanager
from typing import Iterator

import numpy as np
import pandas as pd
import pyomo.environ as pyo
from box import Box
from pandas import Series
from pyomo.common.modeling import NOTSET
from pyomo.core.base.indexed_component import IndexedComponent
from pyomo.environ import ConcreteModel, Model


def run_model(data: Box) -> Box:
    with _disable_index_checking():
        model = _create_model(data)
        optimal = _apply_optimizer(model, data)
        values = _process_results(model, data)
        solution = Box(optimal=optimal, **values)
        return data | solution


@contextmanager
def _disable_index_checking() -> Iterator[None]:
    index_checking_enabled = IndexedComponent._DEFAULT_INDEX_CHECKING_ENABLED
    IndexedComponent._DEFAULT_INDEX_CHECKING_ENABLED = False
    try:
        yield
    finally:
        IndexedComponent._DEFAULT_INDEX_CHECKING_ENABLED = index_checking_enabled


def _create_model(data: Box) -> ConcreteModel:
    model = pyo.ConcreteModel()

    model.market = pyo.Set(
        initialize=data.market_price_euro_per_megawatt_hour.dropna().index,
    )

    model.market_rate = pyo.Param(
        initialize=data.market_rate,
        domain=pyo.Reals,
    )

    model.market_time_unit_minute = pyo.Param(
        initialize=data.market_time_unit_minute,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_import_net_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_import_net_fixed_megawatt = pyo.Param(
        model.market,
        initialize=data.bess_grid_import_net_fixed_megawatt,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_import_net_price_euro_per_megawatt_hour = pyo.Param(
        model.market,
        initialize=data.market_price_euro_per_megawatt_hour.fillna(value=0.0),
        domain=pyo.Reals,
    )

    model.bess_grid_import_gross_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.Reals,
    )

    model.bess_grid_import_matched_megawatt_hour = pyo.Param(
        model.market,
        initialize=data.bess_grid_import_matched_megawatt_hour,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_import_condition = pyo.Param(
        initialize=data.dim_ufi_bess_grid_import is not None,
        domain=pyo.Boolean,
    )

    model.bess_res_import_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_res_import_fixed_megawatt = pyo.Param(
        model.market,
        initialize=data.bess_res_import_fixed_megawatt,
        domain=pyo.NonNegativeReals,
    )

    model.bess_res_import_curtailed_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_res_import_curtailed_price_euro_per_megawatt_hour = pyo.Param(
        model.market,
        initialize=0.0,
        domain=pyo.Reals,
    )

    model.bess_res_import_uncurtailed_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_res_import_uncurtailed_price_euro_per_megawatt_hour = pyo.Param(
        model.market,
        initialize=(
            data.market_price_euro_per_megawatt_hour.where(
                data.market_price_euro_per_megawatt_hour
                >= data.res_export_price_euro_per_megawatt_hour,
                other=0.0,
            )
            if data.bess_res_import_clipping_percent != 100.0
            else 0.0
        ),
        domain=pyo.Reals,
    )

    model.bess_res_import_curtailed_uncurtailed_indicator = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.Binary,
    )

    model.bess_res_import_clipping_percent = pyo.Param(
        initialize=data.bess_res_import_clipping_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_res_import_clipping_percent: (
            0.0 <= bess_res_import_clipping_percent <= 100.0
        ),
    )

    model.bess_res_import_clipping_threshold_megawatt = pyo.Param(
        initialize=data.bess_res_import_clipping_threshold_megawatt,
        domain=pyo.NonNegativeReals,
    )

    model.bess_res_import_clipping_condition = pyo.Param(
        initialize=data.bess_res_import_clipping_percent != 100.0,
        domain=pyo.Boolean,
    )

    model.bess_res_import_priority_indicator = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.Binary,
    )

    model.bess_res_import_priority_condition = pyo.Param(
        initialize=data.bess_res_import_priority,
        domain=pyo.Boolean,
    )

    model.bess_res_import_condition = pyo.Param(
        initialize=data.dim_ufi_bess_res_import is not None,
        domain=pyo.Boolean,
    )

    model.bess_grid_export_net_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_export_net_fixed_megawatt = pyo.Param(
        model.market,
        initialize=data.bess_grid_export_net_fixed_megawatt,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_export_net_price_euro_per_megawatt_hour = pyo.Param(
        model.market,
        initialize=data.market_price_euro_per_megawatt_hour.fillna(value=0.0),
        domain=pyo.Reals,
    )

    model.bess_grid_export_gross_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.Reals,
    )

    model.bess_grid_export_matched_megawatt_hour = pyo.Param(
        model.market,
        initialize=data.bess_grid_export_matched_megawatt_hour,
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_export_limits_megawatt = pyo.Param(
        model.market,
        initialize=(
            pd.Series(
                np.minimum.reduce(
                    [
                        data.bess_grid_export_limits_megawatt,
                        data.grid_export_limits_megawatt,
                        pd.Series(
                            data.grid_export_limit_megawatt,
                            index=data.market_input.index,
                            dtype=float,
                        ),
                    ],
                ),
                index=data.market_input.index,
                dtype=float,
            )
        ),
        domain=pyo.NonNegativeReals,
    )

    model.bess_grid_export_condition = pyo.Param(
        initialize=data.dim_ufi_bess_grid_export is not None,
        domain=pyo.Boolean,
    )

    model.bess_fixed_condition = pyo.Param(
        initialize=(
            bool(model.bess_grid_import_net_fixed_megawatt.extract_values())
            or bool(model.bess_res_import_fixed_megawatt.extract_values())
            or bool(model.bess_grid_export_net_fixed_megawatt.extract_values())
        ),
        domain=pyo.Boolean,
    )

    model.bess_charging_power_capacity_megawatt = pyo.Param(
        initialize=data.bess_power_capacity_megawatt,
        domain=pyo.NonNegativeReals,
    )

    model.bess_discharging_power_capacity_megawatt = pyo.Param(
        initialize=data.bess_power_capacity_megawatt,
        domain=pyo.NonNegativeReals,
    )

    model.bess_energy_capacity_megawatt_hour = pyo.Param(
        initialize=data.bess_energy_capacity_megawatt_hour,
        domain=pyo.NonNegativeReals,
    )

    model.bess_charging_efficiency_percent = pyo.Param(
        initialize=data.bess_charging_efficiency_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_charging_efficiency_percent: (
            0.0 <= bess_charging_efficiency_percent <= 100.0
        ),
    )

    model.bess_discharging_efficiency_percent = pyo.Param(
        initialize=data.bess_discharging_efficiency_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_discharging_efficiency_percent: (
            0.0 <= bess_discharging_efficiency_percent <= 100.0
        ),
    )

    model.bess_charge_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_discharge_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_charge_discharge_indicator = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.Binary,
    )

    model.bess_maximum_cycles_count = pyo.Param(
        initialize=data.market_horizon_day * data.bess_maximum_cycles_count_per_day,
        domain=pyo.NonNegativeReals,
    )

    model.bess_profit_threshold_euro_per_megawatt_hour = pyo.Param(
        initialize=data.bess_profit_threshold_euro_per_megawatt_hour,
        domain=pyo.NonNegativeReals,
    )

    model.bess_minimum_state_of_charge_percent = pyo.Param(
        initialize=data.bess_minimum_state_of_charge_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_minimum_state_of_charge_percent: (
            0.0 <= bess_minimum_state_of_charge_percent <= 100.0
        ),
    )

    model.bess_maximum_state_of_charge_percent = pyo.Param(
        initialize=data.bess_maximum_state_of_charge_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_maximum_state_of_charge_percent: (
            0.0 <= bess_maximum_state_of_charge_percent <= 100.0
        ),
    )

    model.bess_initial_state_of_charge_percent = pyo.Param(
        initialize=(
            np.clip(
                data.bess_initial_state_of_charge_percent,
                a_min=data.bess_minimum_state_of_charge_percent,
                a_max=min(
                    data.bess_maximum_state_of_charge_percent,
                    data.bess_state_of_health_percent * data.bess_availability_percent,
                ),
            )
        ),
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_initial_state_of_charge_percent: (
            0.0 <= bess_initial_state_of_charge_percent <= 100.0
            and model.bess_minimum_state_of_charge_percent
            <= bess_initial_state_of_charge_percent
            <= model.bess_maximum_state_of_charge_percent
        ),
    )

    model.bess_final_state_of_charge_percent = pyo.Param(
        initialize=data.bess_final_state_of_charge_percent
        if data.bess_final_state_of_charge_percent is not None
        else NOTSET,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_final_state_of_charge_percent: (
            0.0 <= bess_final_state_of_charge_percent <= 100.0
            and model.bess_minimum_state_of_charge_percent
            <= bess_final_state_of_charge_percent
            <= model.bess_maximum_state_of_charge_percent
        ),
    )

    model.bess_final_state_of_charge_condition = pyo.Param(
        initialize=data.bess_final_state_of_charge_percent is not None,
    )

    model.bess_state_of_charge_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_state_of_charge_fixed_percent = pyo.Param(
        model.market,
        initialize=data.bess_state_of_charge_fixed_percent,
        domain=pyo.NonNegativeReals,
    )

    model.bess_previous_state_of_charge_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_state_of_health_percent = pyo.Param(
        initialize=data.bess_state_of_health_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_state_of_health_percent: (
            0.0 <= bess_state_of_health_percent <= 100.0
        ),
    )

    model.bess_availability_percent = pyo.Param(
        initialize=data.bess_availability_percent,
        domain=pyo.NonNegativeReals,
        validate=lambda model, bess_availability_percent: (
            0.0 <= bess_availability_percent <= 100.0
        ),
    )

    model.bess_cycles_count = pyo.Var(
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.bess_profit_euro = pyo.Var(
        initialize=0.0,
        domain=pyo.Reals,
    )

    model.res_export_megawatt_hour = pyo.Param(
        model.market,
        initialize=data.res_export_megawatt_hour,
        domain=pyo.NonNegativeReals,
    )

    model.res_grid_export_net_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.NonNegativeReals,
    )

    model.res_grid_export_net_price_euro_per_megawatt_hour = pyo.Param(
        model.market,
        initialize=(
            data.market_price_euro_per_megawatt_hour.where(
                data.market_price_euro_per_megawatt_hour
                >= data.res_export_price_euro_per_megawatt_hour,
                other=0.0,
            )
        ),
        domain=pyo.Reals,
    )

    model.res_grid_export_gross_megawatt_hour = pyo.Var(
        model.market,
        initialize=0.0,
        domain=pyo.Reals,
    )

    model.res_grid_export_matched_megawatt_hour = pyo.Param(
        model.market,
        initialize=data.res_grid_export_matched_megawatt_hour,
        domain=pyo.NonNegativeReals,
    )

    model.res_grid_export_limits_megawatt = pyo.Param(
        model.market,
        initialize=(
            pd.Series(
                np.minimum.reduce(
                    [
                        data.res_grid_export_limits_megawatt,
                        data.grid_export_limits_megawatt,
                        pd.Series(
                            data.grid_export_limit_megawatt,
                            index=data.market_input.index,
                            dtype=float,
                        ),
                    ],
                ),
                index=data.market_input.index,
                dtype=float,
            )
        ),
        domain=pyo.NonNegativeReals,
    )

    model.res_grid_export_condition = pyo.Param(
        initialize=data.dim_ufi_res_grid_export is not None,
        domain=pyo.Boolean,
    )

    model.res_profit_euro = pyo.Var(
        initialize=0.0,
        domain=pyo.Reals,
    )

    model.grid_export_limits_megawatt = pyo.Param(
        model.market,
        initialize=(
            pd.Series(
                np.minimum.reduce(
                    [
                        data.grid_export_limits_megawatt,
                        pd.Series(
                            data.grid_export_limit_megawatt,
                            index=data.market_input.index,
                            dtype=float,
                        ),
                    ],
                ),
                index=data.market_input.index,
                dtype=float,
            )
        ),
        domain=pyo.NonNegativeReals,
    )

    @model.Objective(sense=pyo.maximize)
    def market_rule(model):
        # fmt: off
        return sum(
            math.exp(-model.market_rate * model.market.ord(i))
            * (
                (model.bess_grid_export_net_price_euro_per_megawatt_hour[i] - model.bess_profit_threshold_euro_per_megawatt_hour)
                * model.bess_grid_export_net_megawatt_hour[i]
                - (model.bess_grid_import_net_price_euro_per_megawatt_hour[i] + model.market_rate)
                * model.bess_grid_import_net_megawatt_hour[i]
                - model.bess_res_import_curtailed_price_euro_per_megawatt_hour[i]
                * model.bess_res_import_curtailed_megawatt_hour[i]
                - model.bess_res_import_uncurtailed_price_euro_per_megawatt_hour[i]
                * model.bess_res_import_uncurtailed_megawatt_hour[i]
                + model.res_grid_export_net_price_euro_per_megawatt_hour[i]
                * model.res_grid_export_net_megawatt_hour[i]
            )
            for i in model.market
        )

    @model.Constraint(model.market)
    def bess_grid_import_rule(model, i):
        return (
            model.bess_grid_import_net_megawatt_hour[i]
            == model.bess_grid_import_gross_megawatt_hour[i]
            + model.bess_grid_import_matched_megawatt_hour[i]
        )

    @model.BuildAction(model.market)
    def bess_grid_import_fixed_rule(model, i):
        if not model.bess_fixed_condition:
            return pyo.BuildAction.Skip

        model.bess_grid_import_net_megawatt_hour[i].fix(
            value=model.bess_grid_import_net_fixed_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
            if i in model.bess_grid_import_net_fixed_megawatt
            else 0.0
        )

    @model.BuildAction()
    def bess_grid_import_condition_rule(model):
        if model.bess_grid_import_condition:
            return pyo.BuildAction.Skip

        model.bess_grid_import_net_megawatt_hour.fix(value=0.0)

    @model.Constraint(model.market)
    def bess_res_import_rule(model, i):
        return (
            model.bess_res_import_megawatt_hour[i]
            == model.bess_res_import_curtailed_megawatt_hour[i]
            + model.bess_res_import_uncurtailed_megawatt_hour[i]
        )

    @model.BuildAction(model.market)
    def bess_res_import_fixed_rule(model, i):
        if not model.bess_fixed_condition:
            return pyo.BuildAction.Skip

        model.bess_res_import_megawatt_hour[i].fix(
            value=model.bess_res_import_fixed_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
            if i in model.bess_res_import_fixed_megawatt
            else 0.0
        )

    @model.Constraint(model.market)
    def bess_res_import_curtailed_rule(model, i):
        if model.res_export_megawatt_hour[i] <= (
            model.res_grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        ):
            return model.bess_res_import_curtailed_megawatt_hour[i] <= 0.0

        return model.bess_res_import_curtailed_megawatt_hour[i] <= (
            model.res_export_megawatt_hour[i]
            - model.res_grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        )

    @model.Constraint(model.market)
    def bess_res_import_uncurtailed_rule(model, i):
        if model.res_export_megawatt_hour[i] > (
            model.res_grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        ):
            return model.bess_res_import_uncurtailed_megawatt_hour[i] <= (
                model.res_grid_export_limits_megawatt[i]
                * (model.market_time_unit_minute * (1.0 / 60.0))
            )

        # fmt: off
        return model.bess_res_import_uncurtailed_megawatt_hour[i] <= (
            model.res_export_megawatt_hour[i]
        )

    @model.Constraint(model.market)
    def bess_res_import_curtailed_indicator_rule(model, i):
        return (
            model.bess_res_import_curtailed_megawatt_hour[i]
            <= model.res_export_megawatt_hour[i]
            * model.bess_res_import_curtailed_uncurtailed_indicator[i]
        )

    @model.Constraint(model.market)
    def bess_res_import_uncurtailed_indicator_rule(model, i):
        return (
            model.bess_res_import_uncurtailed_megawatt_hour[i]
            <= model.res_export_megawatt_hour[i]
            * model.bess_res_import_curtailed_uncurtailed_indicator[i]
        )

    @model.Constraint(model.market)
    def bess_res_import_clipping_rule(model, i):
        if not model.bess_res_import_clipping_condition:
            return pyo.Constraint.Skip

        if model.res_export_megawatt_hour[i] <= (
            model.bess_res_import_clipping_threshold_megawatt
            * (model.market_time_unit_minute * (1.0 / 60.0))
        ):
            return model.bess_res_import_megawatt_hour[i] <= 0.0

        return model.bess_res_import_megawatt_hour[i] <= (
            (model.bess_res_import_clipping_percent / 100.0)
            * (
                model.res_export_megawatt_hour[i]
                - model.bess_res_import_clipping_threshold_megawatt
                * (model.market_time_unit_minute * (1.0 / 60.0))
            )
        )

    @model.Constraint(model.market)
    def bess_res_import_priority_res_grid_export_indicator_rule(model, i):
        if not model.bess_res_import_priority_condition:
            return pyo.Constraint.Skip

        return model.res_grid_export_net_megawatt_hour[i] <= (
            model.res_export_megawatt_hour[i]
            * model.bess_res_import_priority_indicator[i]
        )

    @model.Constraint(model.market)
    def bess_res_import_priority_bess_grid_import_indicator_rule(model, i):
        if not model.bess_res_import_priority_condition:
            return pyo.Constraint.Skip

        return model.bess_grid_import_net_megawatt_hour[i] <= (
            model.bess_energy_capacity_megawatt_hour
            * (1 - model.bess_res_import_priority_indicator[i])
        )

    @model.BuildAction()
    def bess_res_import_condition_rule(model):
        if model.bess_res_import_condition:
            return pyo.BuildAction.Skip

        model.bess_res_import_megawatt_hour.fix(value=0.0)

    @model.Constraint(model.market)
    def bess_grid_export_rule(model, i):
        return (
            model.bess_grid_export_net_megawatt_hour[i]
            == model.bess_grid_export_gross_megawatt_hour[i]
            + model.bess_grid_export_matched_megawatt_hour[i]
        )

    @model.BuildAction(model.market)
    def bess_grid_export_fixed_rule(model, i):
        if not model.bess_fixed_condition:
            return pyo.BuildAction.Skip

        model.bess_grid_export_net_megawatt_hour[i].fix(
            value=model.bess_grid_export_net_fixed_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
            if i in model.bess_grid_export_net_fixed_megawatt
            else 0.0
        )

    @model.Constraint(model.market)
    def bess_grid_export_limit_rule(model, i):
        return model.bess_grid_export_net_megawatt_hour[i] <= (
            model.bess_grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        )

    @model.BuildAction()
    def bess_grid_export_condition_rule(model):
        if model.bess_grid_export_condition:
            return pyo.BuildAction.Skip

        model.bess_grid_export_net_megawatt_hour.fix(value=0.0)

    @model.Constraint(model.market)
    def bess_charging_power_capacity_rule(model, i):
        return model.bess_charge_megawatt_hour[i] <= (
            (model.bess_state_of_health_percent / 100.0 * model.bess_availability_percent / 100.0)
            * model.bess_charging_power_capacity_megawatt
            * (model.market_time_unit_minute * (1.0 / 60.0))
        )

    @model.Constraint(model.market)
    def bess_discharging_power_capacity_rule(model, i):
        return model.bess_discharge_megawatt_hour[i] <= (
            (model.bess_state_of_health_percent / 100.0 * model.bess_availability_percent / 100.0)
            * model.bess_discharging_power_capacity_megawatt
            * (model.market_time_unit_minute * (1.0 / 60.0))
        )

    @model.Constraint(model.market)
    def bess_energy_capacity_rule(model, i):
        return (
            model.bess_state_of_charge_megawatt_hour[i]
            <= (model.bess_state_of_health_percent / 100.0 * model.bess_availability_percent / 100.0)
            * model.bess_energy_capacity_megawatt_hour
        )

    @model.Constraint(model.market)
    def bess_charging_efficiency_rule(model, i):
        return model.bess_charge_megawatt_hour[i] == (
            (model.bess_charging_efficiency_percent / 100.0)
            * (
                model.bess_grid_import_net_megawatt_hour[i]
                + model.bess_res_import_megawatt_hour[i]
            )
        )

    @model.Constraint(model.market)
    def bess_discharging_efficiency_rule(model, i):
        return model.bess_discharge_megawatt_hour[i] == (
            (1.0 / (model.bess_discharging_efficiency_percent / 100.0))
            * model.bess_grid_export_net_megawatt_hour[i]
            if model.bess_discharging_efficiency_percent != 0.0
            else 0.0
        )

    @model.Constraint(model.market)
    def bess_charge_indicator_rule(model, i):
        return model.bess_charge_megawatt_hour[i] <= (
            model.bess_energy_capacity_megawatt_hour
            * model.bess_charge_discharge_indicator[i]
        )

    @model.Constraint(model.market)
    def bess_discharge_indicator_rule(model, i):
        return model.bess_discharge_megawatt_hour[i] <= (
            model.bess_energy_capacity_megawatt_hour
            * (1 - model.bess_charge_discharge_indicator[i])
        )

    @model.Constraint()
    def bess_maximum_cycles_rule(model):
        return model.bess_cycles_count <= model.bess_maximum_cycles_count

    @model.Constraint(model.market)
    def bess_minimum_state_of_charge_rule(model, i):
        return (
            model.bess_state_of_charge_megawatt_hour[i]
            >= (model.bess_minimum_state_of_charge_percent / 100.0)
            * model.bess_energy_capacity_megawatt_hour
        )

    @model.Constraint(model.market)
    def bess_maximum_state_of_charge_rule(model, i):
        return (
            model.bess_state_of_charge_megawatt_hour[i]
            <= (model.bess_maximum_state_of_charge_percent / 100.0)
            * model.bess_energy_capacity_megawatt_hour
        )

    @model.Constraint(model.market)
    def bess_initial_state_of_charge_rule(model, i):
        return (
            model.bess_previous_state_of_charge_megawatt_hour[model.market.first()]
            == (model.bess_initial_state_of_charge_percent / 100.0)
            * model.bess_energy_capacity_megawatt_hour
        )

    @model.Constraint()
    def bess_final_state_of_charge_rule(model):
        if not model.bess_final_state_of_charge_condition:
            return pyo.Constraint.Skip

        return (
            model.bess_state_of_charge_megawatt_hour[model.market.last()]
            == (model.bess_final_state_of_charge_percent / 100.0)
            * model.bess_energy_capacity_megawatt_hour
        )

    @model.Constraint(model.market)
    def bess_state_of_charge_rule(model, i):
        return (
            model.bess_state_of_charge_megawatt_hour[i]
            == model.bess_previous_state_of_charge_megawatt_hour[i]
            + model.bess_charge_megawatt_hour[i]
            - model.bess_discharge_megawatt_hour[i]
        )

    @model.BuildAction(model.market)
    def bess_state_of_charge_fixed_rule(model, i):
        if i not in model.bess_state_of_charge_fixed_percent:
            return pyo.BuildAction.Skip

        model.bess_state_of_charge_megawatt_hour[i].fix(
            value=(model.bess_state_of_charge_fixed_percent[i] / 100.0)
            * model.bess_energy_capacity_megawatt_hour
        )

    @model.Constraint(model.market)
    def bess_previous_state_of_charge_rule(model, i):
        if i == model.market.first():
            return pyo.Constraint.Skip

        return (
            model.bess_previous_state_of_charge_megawatt_hour[i]
            == model.bess_state_of_charge_megawatt_hour[model.market.prev(i)]
        )

    @model.Constraint()
    def bess_cycles_rule(model):
        return model.bess_cycles_count == (
            sum(
                model.bess_discharge_megawatt_hour[i]
                * (1.0 / model.bess_energy_capacity_megawatt_hour)
                if model.bess_energy_capacity_megawatt_hour != 0.0
                else 0.0
                for i in model.market
            )
        )

    @model.Constraint()
    def bess_profit_rule(model):
        return model.bess_profit_euro == (
            sum(
                model.bess_grid_export_net_price_euro_per_megawatt_hour[i]
                * model.bess_grid_export_net_megawatt_hour[i]
                - model.bess_grid_import_net_price_euro_per_megawatt_hour[i]
                * model.bess_grid_import_net_megawatt_hour[i]
                - model.bess_res_import_curtailed_price_euro_per_megawatt_hour[i]
                * model.bess_res_import_curtailed_megawatt_hour[i]
                - model.bess_res_import_uncurtailed_price_euro_per_megawatt_hour[i]
                * model.bess_res_import_uncurtailed_megawatt_hour[i]
                for i in model.market
            )
        )

    @model.Constraint(model.market)
    def res_export_rule(model, i):
        if model.res_export_megawatt_hour[i] > (
            model.res_grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        ):
            return (
                model.res_grid_export_net_megawatt_hour[i]
                == model.res_grid_export_limits_megawatt[i]
                * (model.market_time_unit_minute * (1.0 / 60.0))
                - model.bess_res_import_uncurtailed_megawatt_hour[i]
            )

        return (
            model.res_grid_export_net_megawatt_hour[i]
            == model.res_export_megawatt_hour[i]
            - model.bess_res_import_uncurtailed_megawatt_hour[i]
        )

    @model.Constraint(model.market)
    def res_grid_export_rule(model, i):
        return (
            model.res_grid_export_net_megawatt_hour[i]
            == model.res_grid_export_gross_megawatt_hour[i]
            + model.res_grid_export_matched_megawatt_hour[i]
        )

    @model.Constraint(model.market)
    def res_grid_export_limit_rule(model, i):
        return model.res_grid_export_net_megawatt_hour[i] <= (
            model.res_grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        )

    @model.BuildAction()
    def res_grid_export_condition_rule(model):
        if model.res_grid_export_condition:
            return pyo.BuildAction.Skip

        model.res_grid_export_net_megawatt_hour.fix(value=0.0)

    @model.Constraint()
    def res_profit_rule(model):
        return model.res_profit_euro == (
            sum(
                model.res_grid_export_net_price_euro_per_megawatt_hour[i]
                * model.res_grid_export_net_megawatt_hour[i]
                for i in model.market
            )
        )

    @model.Constraint(model.market)
    def grid_export_limit_rule(model, i):
        return (
            model.res_grid_export_net_megawatt_hour[i]
            + model.bess_grid_export_net_megawatt_hour[i]
        ) <= (
            model.grid_export_limits_megawatt[i]
            * (model.market_time_unit_minute * (1.0 / 60.0))
        )

    return model


def _apply_optimizer(model: Model, data: Box) -> bool:
    with pyo.SolverFactory(data.solver) as opt:
        results = opt.solve(model)
        optimal = pyo.check_optimal_termination(results)
        return optimal


def _process_results(model: Model, data: Box) -> dict[str, float | Series[float]]:
    values = {}
    for component in model.component_objects(ctype=pyo.Var):
        value = pd.Series(data=component.extract_values(), dtype=float)
        value = value.mask(np.isclose(value, 0.0), other=0.0)
        value = value.item() if not component.is_indexed() else value.reindex(index=data.market_input.index)  # fmt: off
        values[component.local_name] = value
    return values
