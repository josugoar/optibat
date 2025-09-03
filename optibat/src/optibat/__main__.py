import logging
import math
import runpy
import sys
import tempfile
import uuid
from datetime import date, timedelta
from importlib.resources import as_file, files
from pathlib import Path
from typing import ContextManager

import pandas as pd
import streamlit as st
from box import Box
from dynaconf.loaders import yaml_loader as loader
from filelock import FileLock
from streamlit import runtime
from streamlit import session_state as ss

import optibat

logger = logging.getLogger(name=__name__)


def main() -> None:
    if optibat.settings.headless:
        if not optibat.settings.auto_enabled:
            logger.info("Automatic execution is disabled, aborting scheduled run")
            return
        try:
            data = Box({key.lower(): value for key, value in optibat.settings.as_dict().items()})  # fmt: off
            optibat.optibat(data)
        except Exception as e:
            logger.exception(e)
        finally:
            return

    if not runtime.exists():
        with _read_main_path() as main_path:
            sys.argv = [
                "streamlit",
                "run",
                str(main_path),
                "--client.showErrorDetails",
                "none",
                "--browser.gatherUsageStats",
                "False",
                "--server.headless",
                "True",
                "--theme.primaryColor",
                "#00a443",
            ]
            runpy.run_module("streamlit", run_name="__main__")
            return

    with _read_favicon_path() as favicon_path:
        st.set_page_config(
            page_title="Optimización de Baterías en Tiempo Real",
            page_icon=str(favicon_path),
            initial_sidebar_state="expanded",
        )

    if "login" not in ss:
        ss.login = False if optibat.settings.auth is not None else True

    if not ss.login:
        with st.form("login_form"):
            st.header("Iniciar Sesión")

            user = st.text_input(
                "Usuario",
                autocomplete="username",
            )

            password = st.text_input(
                "Contraseña",
                type="password",
                autocomplete="current-password",
            )

            login_submitted = st.form_submit_button(label="Acceder")

        if not login_submitted:
            st.stop()

        if not user and not password:
            st.warning("Introduzca su usuario y contraseña.")
            st.stop()

        if not user:
            st.warning("Introduzca su usuario.")
            st.stop()

        if not password:
            st.warning("Introduzca su contraseña.")
            st.stop()

        login = optibat.login(user, password, optibat.settings.auth.name)

        if not login:
            st.error("Inicio de sesión fallido.")
            st.stop()

        ss.login = login
        st.rerun()

    if "modules" not in ss:
        optibat.settings.reload()
        optibat.settings.validators.validate_all()
        ss.modules = {
            module: optibat.settings.from_env(env=module, keep=True)
            for module in optibat.settings.modules
        }

    if "module" not in ss:
        ss.module = (
            optibat.settings.current_env
            if optibat.settings.current_env in ss.modules
            else next(iter(ss.modules), None)
        )

    if "settings" not in ss:
        ss.settings = ss.modules[ss.module] if ss.modules else optibat.settings

    if "data" not in ss:
        ss.data = None

    if "run" not in ss:
        ss.run = False

    if "bess_grid_import_net_fixed_megawatt" not in ss:
        ss.bess_grid_import_net_fixed_megawatt = None

    if "bess_res_import_fixed_megawatt" not in ss:
        ss.bess_res_import_fixed_megawatt = None

    if "bess_grid_export_net_fixed_megawatt" not in ss:
        ss.bess_grid_export_net_fixed_megawatt = None

    if "manual_positions_megawatt_key" not in ss:
        ss.manual_positions_megawatt_key = uuid.uuid4().hex

    if "manual_positions_megawatt_changed" not in ss:
        ss.manual_positions_megawatt_changed = False

    if "manual_positions_megawatt_hour_key" not in ss:
        ss.manual_positions_megawatt_hour_key = uuid.uuid4().hex

    if "manual_positions_megawatt_hour_changed" not in ss:
        ss.manual_positions_megawatt_hour_changed = False

    if "bess_state_of_charge_fixed_percent" not in ss:
        ss.bess_state_of_charge_fixed_percent = None

    if "manual_state_of_charge_percent_key" not in ss:
        ss.manual_state_of_charge_percent_key = uuid.uuid4().hex

    if "manual_state_of_charge_percent_changed" not in ss:
        ss.manual_state_of_charge_percent_changed = False

    if "manual_state_of_charge_megawatt_hour_key" not in ss:
        ss.manual_state_of_charge_megawatt_hour_key = uuid.uuid4().hex

    if "manual_state_of_charge_megawatt_hour_changed" not in ss:
        ss.manual_state_of_charge_megawatt_hour_changed = False

    if "market_date" not in ss:
        ss.market_date = date.today() + timedelta(days=1)

    if "market_type" not in ss:
        ss.market_type = "MD"

    if "market_horizon_day" not in ss:
        ss.market_horizon_day = ss.settings.market_horizon_day

    if "market_forecast" not in ss:
        ss.market_forecast = ss.settings.market_forecast

    if "market_csv" not in ss:
        ss.market_csv = None

    if "auto_enabled" not in ss:
        ss.auto_enabled = ss.settings.auto_enabled

    if "reset" not in ss:
        ss.reset = True

    if ss.reset:
        # fmt: off
        ss.bess_power_capacity_megawatt = ss.settings.bess_power_capacity_megawatt
        ss.bess_energy_capacity_megawatt_hour = ss.settings.bess_energy_capacity_megawatt_hour
        ss.bess_charging_efficiency_percent = ss.settings.bess_charging_efficiency_percent
        ss.bess_discharging_efficiency_percent = ss.settings.bess_discharging_efficiency_percent
        ss.bess_maximum_cycles_count_per_day = ss.settings.bess_maximum_cycles_count_per_day
        ss.bess_profit_threshold_euro_per_megawatt_hour = ss.settings.bess_profit_threshold_euro_per_megawatt_hour
        ss.bess_minimum_state_of_charge_percent = ss.settings.bess_minimum_state_of_charge_percent
        ss.bess_maximum_state_of_charge_percent = ss.settings.bess_maximum_state_of_charge_percent
        ss.bess_initial_state_of_charge_percent = ss.settings.bess_initial_state_of_charge_percent
        ss.bess_final_state_of_charge_percent = ss.settings.bess_final_state_of_charge_percent
        ss.bess_purchase_tolerance_euro_per_megawatt_hour = ss.settings.bess_purchase_tolerance_euro_per_megawatt_hour
        ss.bess_sale_tolerance_euro_per_megawatt_hour = ss.settings.bess_sale_tolerance_euro_per_megawatt_hour
        ss.reset = False

    # fmt: off
    ss.settings.market_date = ss.market_date
    ss.settings.market_horizon_day = ss.market_horizon_day
    ss.settings.market_type = ss.market_type
    ss.settings.market_forecast = ss.market_forecast
    ss.settings.market_csv = ss.market_csv
    ss.settings.auto_enabled = ss.auto_enabled
    ss.settings.bess_power_capacity_megawatt = ss.bess_power_capacity_megawatt
    ss.settings.bess_energy_capacity_megawatt_hour = ss.bess_energy_capacity_megawatt_hour
    ss.settings.bess_charging_efficiency_percent = ss.bess_charging_efficiency_percent
    ss.settings.bess_discharging_efficiency_percent = ss.bess_discharging_efficiency_percent
    ss.settings.bess_maximum_cycles_count_per_day = ss.bess_maximum_cycles_count_per_day
    ss.settings.bess_profit_threshold_euro_per_megawatt_hour = ss.bess_profit_threshold_euro_per_megawatt_hour
    ss.settings.bess_minimum_state_of_charge_percent = ss.bess_minimum_state_of_charge_percent
    ss.settings.bess_maximum_state_of_charge_percent = ss.bess_maximum_state_of_charge_percent
    ss.settings.bess_initial_state_of_charge_percent = ss.bess_initial_state_of_charge_percent
    ss.settings.bess_final_state_of_charge_percent = ss.bess_final_state_of_charge_percent
    ss.settings.bess_purchase_tolerance_euro_per_megawatt_hour = ss.bess_purchase_tolerance_euro_per_megawatt_hour
    ss.settings.bess_sale_tolerance_euro_per_megawatt_hour = ss.bess_sale_tolerance_euro_per_megawatt_hour
    # fmt: on

    with _read_icon_path() as icon_path, _read_logo_path() as logo_path:
        st.logo(str(logo_path), size="large", icon_image=str(icon_path))

    manual_changed = (
        ss.manual_positions_megawatt_changed
        or ss.manual_positions_megawatt_hour_changed
        or ss.manual_state_of_charge_percent_changed
        or ss.manual_state_of_charge_megawatt_hour_changed
    )

    with st.sidebar:
        st.title("Optimización de Baterías en Tiempo Real")

        st.selectbox(
            "Instalación",
            ss.modules.keys(),
            key="module",
            on_change=_on_change_module,
            disabled=manual_changed,
        )

        with st.popover(
            "Configuración",
            disabled=manual_changed,
        ):
            with st.container(height=185):
                settings_left_column, settings_right_column = st.columns(2)

                with settings_left_column:
                    st.number_input(
                        "Potencia Máxima (MW)",
                        min_value=0.0,
                        max_value=None,
                        key="bess_power_capacity_megawatt",
                    )

                    st.number_input(
                        "Rendimiento de Carga (%)",
                        min_value=0.0,
                        max_value=100.0,
                        key="bess_charging_efficiency_percent",
                    )

                    st.number_input(
                        "Ciclos Máximos Diarios",
                        min_value=0.0,
                        max_value=None,
                        key="bess_maximum_cycles_count_per_day",
                    )

                    st.number_input(
                        "Estado de Carga Mínimo (%)",
                        min_value=0.0,
                        max_value=100.0,
                        key="bess_minimum_state_of_charge_percent",
                        on_change=_on_change_minimum_state_of_charge_percent,
                    )

                    st.number_input(
                        "Estado de Carga Máximo (%)",
                        min_value=0.0,
                        max_value=100.0,
                        key="bess_maximum_state_of_charge_percent",
                        on_change=_on_change_maximum_state_of_charge_percent,
                    )

                    st.number_input(
                        "Tolerancia de Compra (€/MWh)",
                        min_value=None,
                        max_value=None,
                        key="bess_purchase_tolerance_euro_per_megawatt_hour",
                    )

                with settings_right_column:
                    st.number_input(
                        "Energía Almacenada Máxima (MWh)",
                        min_value=0.0,
                        max_value=None,
                        key="bess_energy_capacity_megawatt_hour",
                    )

                    st.number_input(
                        "Rendimiento de Descarga (%)",
                        min_value=0.0,
                        max_value=100.0,
                        key="bess_discharging_efficiency_percent",
                    )

                    st.number_input(
                        "Spread Objetivo (€/MWh)",
                        min_value=0.0,
                        max_value=None,
                        key="bess_profit_threshold_euro_per_megawatt_hour",
                    )

                    st.number_input(
                        "Estado de Carga Inicial (%)",
                        min_value=ss.bess_minimum_state_of_charge_percent,
                        max_value=ss.bess_maximum_state_of_charge_percent,
                        key="bess_initial_state_of_charge_percent",
                    )

                    st.number_input(
                        "Estado de Carga Final (%)",
                        min_value=ss.bess_minimum_state_of_charge_percent,
                        max_value=ss.bess_maximum_state_of_charge_percent,
                        key="bess_final_state_of_charge_percent",
                    )

                    st.number_input(
                        "Tolerancia de Venta (€/MWh)",
                        min_value=None,
                        max_value=None,
                        key="bess_sale_tolerance_euro_per_megawatt_hour",
                    )

                st.button("Restablecer", on_click=_on_click_reset)

        st.header("Selección de Mercado")

        st.date_input(
            "Fecha",
            min_value=date(year=2025, month=3, day=19),
            max_value=None,
            key="market_date",
            disabled=manual_changed,
        )

        st.selectbox(
            "Tipo",
            ["MD", "MI1", "MI2", "MI3", "MIC"],
            key="market_type",
            disabled=manual_changed,
        )

        st.slider(
            "Horizonte (días)",
            min_value=1,
            max_value=7,
            key="market_horizon_day",
            disabled=manual_changed,
        )

        st.radio(
            "Previsión",
            ["XXXX_XXXX", "XXXX_XXXX"],
            key="market_forecast",
            disabled=manual_changed,
            horizontal=True,
        )

        st.button(
            "Ejecutar",
            on_click=_on_click_run,
            type="primary" if not ss.run else "secondary",
            disabled=ss.settings.market is None and ss.settings.market_csv is None,
        )

        if ss.settings.output_csv_path is not None:
            with st.popover(
                "Cargar",
                disabled=manual_changed,
            ):
                st.file_uploader(
                    "CSV",
                    type="csv",
                    key="input_csv",
                    on_change=_on_change_input_csv,
                    label_visibility="collapsed",
                )

        with st.popover("Descargar"):
            if ss.data is not None:
                output_columns_count = sum(
                    [
                        ss.settings.output_XXXX_XXXX_path is not None,
                        ss.settings.output_XXXX_XXXX_path is not None,
                        ss.settings.output_csv_path is not None,
                    ]
                )

                XXXX_XXXX_column, XXXX_XXXX_column, csv_column, *_ = [
                    *st.columns(output_columns_count),
                    None,
                    None,
                    None,
                ]

                if ss.settings.output_XXXX_XXXX_path is not None:
                    with XXXX_XXXX_column:
                        # fmt: off
                        st.download_button(
                            "XXXX_XXXX",
                            ss.data.output_XXXX_XXXX,
                            file_name=Path(ss.data.output_XXXX_XXXX_path.format(ss.data.current_datetime)).name
                            if ss.data.output_XXXX_XXXX_path is not None
                            else None,
                            mime="text/csv",
                            on_click="ignore",
                            use_container_width=True,
                        )

                if ss.settings.output_XXXX_XXXX_path is not None:
                    with XXXX_XXXX_column:
                        # fmt: off
                        st.download_button(
                            "XXXX_XXXX",
                            ss.data.output_XXXX_XXXX,
                            file_name=Path(ss.data.output_XXXX_XXXX_path.format(ss.data.current_datetime)).name
                            if ss.data.output_XXXX_XXXX_path is not None
                            else None,
                            mime="text/csv",
                            on_click="ignore",
                            use_container_width=True,
                        )

                if ss.settings.output_csv_path is not None:
                    with csv_column:
                        # fmt: off
                        st.download_button(
                            "CSV",
                            ss.data.output_csv,
                            file_name=Path(ss.data.output_csv_path.format(ss.data.current_datetime)).name
                            if ss.data.output_csv_path is not None
                            else None,
                            mime="text/csv",
                            on_click="ignore",
                            use_container_width=True,
                        )

        # fmt: off
        st.button(
            "Guardar",
            on_click=_on_click_save,
            disabled=ss.data is None or (ss.data.output_XXXX_XXXX_path is None and ss.data.output_XXXX_XXXX_path is None and ss.data.output_csv_path is None),
        )
        # fmt: on

        st.toggle(
            "Guardado Automático",
            key="auto_enabled",
            on_change=_on_change_auto_enabled,
        )

        if ss.data is not None and not ss.data.optimal:
            st.warning("No se pudo encontrar una solución óptima.")

        st.caption(
            """
            Optimización de Baterías en Tiempo Real
            <br>
            XXXX_XXXX.
            """,
            unsafe_allow_html=True,
        )

        if ss.settings.auth:
            st.html(
                """
                <style>
                    .st-key-logout {
                        opacity: 0.6;
                    }
                </style>
                """
            )

            if st.button("Cerrar Sesión", key="logout", type="tertiary"):
                ss.clear()
                st.rerun()

    if ss.data is not None:
        (
            profit_euro_per_megawatt_hour_tab,
            profit_euro_per_megawatt_tab,
            profit_euro_tab,
        ) = st.tabs(["€/MWh", "€/MW", "€"])

        with profit_euro_per_megawatt_hour_tab:
            (
                profit_euro_per_megawatt_hour_column,
                cycle_profit_euro_per_megawatt_hour_column,
                cycles_column,
            ) = st.columns(3, border=True)

            with profit_euro_per_megawatt_hour_column:
                profit_euro_per_megawatt_hour = (
                    ss.data.bess_profit_euro
                    / ss.data.bess_grid_export_net_megawatt_hour.sum()
                    if ss.data.bess_grid_export_net_megawatt_hour.sum() != 0.0
                    else 0.0
                )

                delta_profit_euro_per_megawatt_hour = (
                    profit_euro_per_megawatt_hour
                    - ss.data.bess_profit_threshold_euro_per_megawatt_hour
                )

                st.metric(
                    "Ingreso [€/MWh]",
                    value=f"{profit_euro_per_megawatt_hour:z.1f}",
                    delta=f"{delta_profit_euro_per_megawatt_hour:z.1f}"
                    if f"{delta_profit_euro_per_megawatt_hour:z.1f}" != "0.0"
                    else None,
                    delta_color="normal",
                )

            with cycle_profit_euro_per_megawatt_hour_column:
                cycle_profit_euro_per_megawatt_hour = (
                    ss.data.bess_profit_euro
                    / ss.data.bess_cycles_count
                    / ss.data.bess_grid_export_net_megawatt_hour.sum()
                    if ss.data.bess_cycles_count != 0.0
                    else 0.0
                )

                delta_cycle_profit_euro_per_megawatt_hour = (
                    cycle_profit_euro_per_megawatt_hour
                    - ss.data.bess_profit_threshold_euro_per_megawatt_hour
                    / ss.data.bess_cycles_count
                    if ss.data.bess_cycles_count != 0.0
                    else 0.0
                )

                st.metric(
                    "Ingreso por Ciclo [€/MWh]",
                    value=f"{cycle_profit_euro_per_megawatt_hour:z.1f}",
                    delta=f"{delta_cycle_profit_euro_per_megawatt_hour:z.1f}"
                    if f"{delta_cycle_profit_euro_per_megawatt_hour:z.1f}" != "0.0"
                    else None,
                    delta_color="normal",
                )

            with cycles_column:
                cycles_count = ss.data.bess_cycles_count

                delta_cycles_count = (
                    cycles_count
                    - ss.data.market_horizon_day
                    * ss.data.bess_maximum_cycles_count_per_day
                )

                st.metric(
                    "Ciclos",
                    value=f"{cycles_count:z.1f}",
                    delta=f"{delta_cycles_count:z.1f}"
                    if f"{delta_cycles_count:z.1f}" != "0.0"
                    else None,
                    delta_color="inverse",
                )

        with profit_euro_per_megawatt_tab:
            (
                profit_megawatt_column,
                cycle_profit_megawatt_column,
                cycles_column,
            ) = st.columns(3, border=True)

            with profit_megawatt_column:
                profit_euro_per_megawatt = (
                    ss.data.bess_profit_euro
                    / ss.data.bess_grid_export_net_megawatt_hour.sum()
                    * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                    if ss.data.bess_grid_export_net_megawatt_hour.sum() != 0.0
                    else 0.0
                )

                delta_profit_euro_per_megawatt = (
                    profit_euro_per_megawatt
                    - ss.data.bess_profit_threshold_euro_per_megawatt_hour
                    * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                )

                st.metric(
                    "Ingreso [€/MW]",
                    value=f"{profit_euro_per_megawatt:z.1f}",
                    delta=f"{delta_profit_euro_per_megawatt:z.1f}"
                    if f"{delta_profit_euro_per_megawatt:z.1f}" != "0.0"
                    else None,
                    delta_color="normal",
                )

            with cycle_profit_megawatt_column:
                cycle_profit_euro_per_megawatt = (
                    ss.data.bess_profit_euro
                    / ss.data.bess_cycles_count
                    / ss.data.bess_grid_export_net_megawatt_hour.sum()
                    * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                    if ss.data.bess_cycles_count != 0.0
                    else 0.0
                )

                delta_cycle_profit_euro_per_megawatt = (
                    cycle_profit_euro_per_megawatt
                    - ss.data.bess_profit_threshold_euro_per_megawatt_hour
                    / ss.data.bess_cycles_count
                    * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                    if ss.data.bess_cycles_count != 0.0
                    else 0.0
                )

                st.metric(
                    "Ingreso por Ciclo [€/MW]",
                    value=f"{cycle_profit_euro_per_megawatt:z.1f}",
                    delta=f"{delta_cycle_profit_euro_per_megawatt:z.1f}"
                    if f"{delta_cycle_profit_euro_per_megawatt:z.1f}" != "0.0"
                    else None,
                    delta_color="normal",
                )

            with cycles_column:
                cycles_count = ss.data.bess_cycles_count

                delta_cycles_count = (
                    cycles_count
                    - ss.data.market_horizon_day
                    * ss.data.bess_maximum_cycles_count_per_day
                )

                st.metric(
                    "Ciclos",
                    value=f"{cycles_count:z.1f}",
                    delta=f"{delta_cycles_count:z.1f}"
                    if f"{delta_cycles_count:z.1f}" != "0.0"
                    else None,
                    delta_color="inverse",
                )

        with profit_euro_tab:
            (
                profit_euro,
                cycle_profit_euro,
                cycles_column,
            ) = st.columns(3, border=True)

            with profit_euro:
                profit_euro = ss.data.bess_profit_euro

                delta_profit_euro = (
                    profit_euro
                    - ss.data.bess_profit_threshold_euro_per_megawatt_hour
                    * ss.data.bess_grid_export_net_megawatt_hour.sum()
                )

                st.metric(
                    "Ingreso [€]",
                    value=f"{profit_euro:z.1f}",
                    delta=f"{delta_profit_euro:z.1f}"
                    if f"{delta_profit_euro:z.1f}" != "0.0"
                    else None,
                    delta_color="normal",
                )

            with cycle_profit_euro:
                cycle_profit_euro = (
                    ss.data.bess_profit_euro / ss.data.bess_cycles_count
                    if ss.data.bess_cycles_count != 0.0
                    else 0.0
                )

                delta_cycle_profit_euro = (
                    cycle_profit_euro
                    - ss.data.bess_profit_threshold_euro_per_megawatt_hour
                    / ss.data.bess_cycles_count
                    * ss.data.bess_grid_export_net_megawatt_hour.sum()
                    if ss.data.bess_cycles_count != 0.0
                    else 0.0
                )

                st.metric(
                    "Ingreso por Ciclo [€]",
                    value=f"{cycle_profit_euro:z.1f}",
                    delta=f"{delta_cycle_profit_euro:z.1f}"
                    if f"{delta_cycle_profit_euro:z.1f}" != "0.0"
                    else None,
                    delta_color="normal",
                )

            with cycles_column:
                cycles_count = ss.data.bess_cycles_count

                delta_cycles_count = (
                    cycles_count
                    - ss.data.market_horizon_day
                    * ss.data.bess_maximum_cycles_count_per_day
                )

                st.metric(
                    "Ciclos",
                    value=f"{cycles_count:z.1f}",
                    delta=f"{delta_cycles_count:z.1f}"
                    if f"{delta_cycles_count:z.1f}" != "0.0"
                    else None,
                    delta_color="inverse",
                )

        _add_vertical_space(num_lines=3)

        # fmt: off
        price_euro_per_megawatt_hour = {
            "Compra": (
                ss.data.bess_price_euro_per_megawatt_hour
                .where(ss.data.bess_discharge_megawatt_hour - ss.data.bess_charge_megawatt_hour < 0.0)
                .bfill().ffill()
                .where(ss.data.market_price_euro_per_megawatt_hour.notna())
            ),
            "Venta": (
                ss.data.bess_price_euro_per_megawatt_hour
                .where(ss.data.bess_discharge_megawatt_hour - ss.data.bess_charge_megawatt_hour > 0.0)
                .bfill().ffill()
                .where(ss.data.market_price_euro_per_megawatt_hour.notna())
            ),
            "​Mercado": ss.data.market_price_euro_per_megawatt_hour,
        }
        # fmt: on

        price_euro_per_megawatt_hour_color = {
            "Compra": "#ff9c1a55",
            "Venta": "#0da9ff55",
            "​Mercado": "#00a443aa",
        }

        st.line_chart(
            data=price_euro_per_megawatt_hour,
            y_label="Precio [€/MWh]",
            color=list(price_euro_per_megawatt_hour_color.values()),
        )

        if ss.data.dim_ufi_bess_res_import is not None:
            (
                energy_megawatt_tab,
                energy_megawatt_hour_tab,
            ) = st.tabs(["MW", "MWh"])

            with energy_megawatt_tab:
                energy_megawatt = {
                    "Bruto": (
                        ss.data.res_export_megawatt_hour
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                    ),
                    "Neto": (
                        (
                            ss.data.res_export_megawatt_hour
                            - ss.data.bess_res_import_megawatt_hour.fillna(value=0.0)
                        )
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                    ),
                }

                energy_megawatt_color = {
                    "Bruto": "#00a44355",
                    "Neto": "#00a443aa",
                }

                if ss.data.bess_res_import_megawatt_hour.sum() == 0.0:
                    del energy_megawatt["Bruto"]
                    del energy_megawatt_color["Bruto"]

                st.area_chart(
                    data=energy_megawatt,
                    y_label="Generación Renovable [MW]",
                    color=list(energy_megawatt_color.values()),
                )

            with energy_megawatt_hour_tab:
                energy_megawatt_hour = {
                    "Bruto": ss.data.res_export_megawatt_hour,
                    "Neto": (
                        ss.data.res_export_megawatt_hour
                        - ss.data.bess_res_import_megawatt_hour.fillna(value=0.0)
                    ),
                }

                energy_megawatt_hour_color = {
                    "Bruto": "#00a44355",
                    "Neto": "#00a443aa",
                }

                if ss.data.bess_res_import_megawatt_hour.sum() == 0.0:
                    del energy_megawatt_hour["Bruto"]
                    del energy_megawatt_hour_color["Bruto"]

                st.area_chart(
                    data=energy_megawatt_hour,
                    y_label="Generación Renovable [MWh]",
                    color=list(energy_megawatt_hour_color.values()),
                )

        (
            auto_positions_tab,
            manual_positions_tab,
        ) = st.tabs(["Óptimo", "Manual"])

        with auto_positions_tab:
            (
                auto_positions_megawatt_tab,
                auto_positions_megawatt_hour_tab,
            ) = st.tabs(["MW", "MWh"])

            with auto_positions_megawatt_tab:
                auto_positions_megawatt = {
                    "Carga Red": (
                        -ss.data.bess_grid_import_gross_megawatt_hour
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                        if ss.data.market_time_unit_minute != 0.0
                        else 0.0
                    ),
                    "Carga Red Casada": (
                        -ss.data.bess_grid_import_matched_megawatt_hour
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                        if ss.data.market_time_unit_minute != 0.0
                        else 0.0
                    ),
                    "Carga Renovable": (
                        -ss.data.bess_res_import_megawatt_hour
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                        if ss.data.market_time_unit_minute != 0.0
                        else 0.0
                    ),
                    "Descarga Red": (
                        ss.data.bess_grid_export_gross_megawatt_hour
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                        if ss.data.market_time_unit_minute != 0.0
                        else 0.0
                    ),
                    "Descarga Red Casada": (
                        ss.data.bess_grid_export_matched_megawatt_hour
                        * (1.0 / (ss.data.market_time_unit_minute * (1.0 / 60.0)))
                        if ss.data.market_time_unit_minute != 0.0
                        else 0.0
                    ),
                }

                auto_positions_megawatt_color = {
                    "Carga Red": "#ff9c1aaa",
                    "Carga Red Casada": "#ff9c1a55",
                    "Carga Renovable": "#00a443aa",
                    "Descarga Red": "#0da9ffaa",
                    "Descarga Red Casada": "#0da9ff55",
                }

                if ss.data.dim_ufi_bess_grid_import is None:
                    del auto_positions_megawatt["Carga Red"]
                    del auto_positions_megawatt_color["Carga Red"]

                    del auto_positions_megawatt["Carga Red Casada"]
                    del auto_positions_megawatt_color["Carga Red Casada"]

                if ss.data.dim_ufi_bess_res_import is None:
                    del auto_positions_megawatt["Carga Renovable"]
                    del auto_positions_megawatt_color["Carga Renovable"]

                if ss.data.dim_ufi_bess_grid_export is None:
                    del auto_positions_megawatt["Descarga Red"]
                    del auto_positions_megawatt_color["Descarga Red"]

                    del auto_positions_megawatt["Descarga Red Casada"]
                    del auto_positions_megawatt_color["Descarga Red Casada"]

                st.bar_chart(
                    data=auto_positions_megawatt,
                    y_label="Programación [MW]",
                    color=list(auto_positions_megawatt_color.values()),
                    stack=True,
                )

            with auto_positions_megawatt_hour_tab:
                auto_positions_megawatt_hour = {
                    "Carga Red": -ss.data.bess_grid_import_gross_megawatt_hour,
                    "Carga Red Casada": -ss.data.bess_grid_import_matched_megawatt_hour,
                    "Carga Renovable": -ss.data.bess_res_import_megawatt_hour,
                    "Descarga Red": ss.data.bess_grid_export_gross_megawatt_hour,
                    "Descarga Red Casada": ss.data.bess_grid_export_matched_megawatt_hour,
                }

                auto_positions_megawatt_hour_color = {
                    "Carga Red": "#ff9c1aaa",
                    "Carga Red Casada": "#ff9c1a55",
                    "Carga Renovable": "#00a443aa",
                    "Descarga Red": "#0da9ffaa",
                    "Descarga Red Casada": "#0da9ff55",
                }

                if ss.data.dim_ufi_bess_grid_import is None:
                    del auto_positions_megawatt_hour["Carga Red"]
                    del auto_positions_megawatt_hour_color["Carga Red"]

                    del auto_positions_megawatt_hour["Carga Red Casada"]
                    del auto_positions_megawatt_hour_color["Carga Red Casada"]

                if ss.data.dim_ufi_bess_res_import is None:
                    del auto_positions_megawatt_hour["Carga Renovable"]
                    del auto_positions_megawatt_hour_color["Carga Renovable"]

                if ss.data.dim_ufi_bess_grid_export is None:
                    del auto_positions_megawatt_hour["Descarga Red"]
                    del auto_positions_megawatt_hour_color["Descarga Red"]

                    del auto_positions_megawatt_hour["Descarga Red Casada"]
                    del auto_positions_megawatt_hour_color["Descarga Red Casada"]

                st.bar_chart(
                    data=auto_positions_megawatt_hour,
                    y_label="Programación [MWh]",
                    color=list(auto_positions_megawatt_hour_color.values()),
                    stack=True,
                )

        with manual_positions_tab:
            (
                manual_positions_megawatt_tab,
                manual_positions_megawatt_hour_tab,
            ) = st.tabs(["MW", "MWh"])

            with manual_positions_megawatt_tab:
                manual_positions_megawatt = {
                    "Carga Red [MW]": (
                        pd.Series(
                            data=ss.data.bess_grid_import_net_fixed_megawatt
                            if not ss.get("manual_positions_megawatt_clear", False)
                            else ss.settings.bess_grid_import_net_fixed_megawatt,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                    ),
                    "Carga Renovable [MW]": (
                        pd.Series(
                            data=ss.data.bess_res_import_fixed_megawatt
                            if not ss.get("manual_positions_megawatt_clear", False)
                            else ss.settings.bess_res_import_fixed_megawatt,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                    ),
                    "Descarga Red [MW]": (
                        pd.Series(
                            data=ss.data.bess_grid_export_net_fixed_megawatt
                            if not ss.get("manual_positions_megawatt_clear", False)
                            else ss.settings.bess_grid_export_net_fixed_megawatt,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                    ),
                }

                if ss.data.dim_ufi_bess_grid_import is None:
                    del manual_positions_megawatt["Carga Red [MW]"]

                if ss.data.dim_ufi_bess_res_import is None:
                    del manual_positions_megawatt["Carga Renovable [MW]"]

                if ss.data.dim_ufi_bess_grid_export is None:
                    del manual_positions_megawatt["Descarga Red [MW]"]

                ss.manual_positions_megawatt = st.data_editor(
                    manual_positions_megawatt,
                    column_config={"_index": ""},
                    disabled=not ss.run and not ss.manual_positions_megawatt_changed,
                    key=f"manual_positions_megawatt_{ss.manual_positions_megawatt_hour_key}",
                    on_change=_on_change_manual_positions_megawatt,
                )

                st.button(
                    "Borrar",
                    key="manual_positions_megawatt_clear",
                    on_click=_on_change_manual_positions_megawatt,
                    disabled=not ss.run and not ss.manual_positions_megawatt_changed,
                )

                st.button(
                    "Restablecer",
                    key="manual_positions_megawatt_reset",
                    on_click=_on_click_manual_positions_megawatt_reset,
                    disabled=not ss.run and not ss.manual_positions_megawatt_changed,
                )

            with manual_positions_megawatt_hour_tab:
                manual_positions_megawatt_hour = {
                    "Carga Red [MWh]": (
                        pd.Series(
                            data=ss.data.bess_grid_import_net_fixed_megawatt
                            if not ss.get("manual_positions_megawatt_hour_clear", False)
                            else ss.settings.bess_grid_import_net_fixed_megawatt,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                        * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                    ),
                    "Carga Renovable [MWh]": (
                        pd.Series(
                            data=ss.data.bess_res_import_fixed_megawatt
                            if not ss.get("manual_positions_megawatt_hour_clear", False)
                            else ss.settings.bess_res_import_fixed_megawatt,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                        * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                    ),
                    "Descarga Red [MWh]": (
                        pd.Series(
                            data=ss.data.bess_grid_export_net_fixed_megawatt
                            if not ss.get("manual_positions_megawatt_hour_clear", False)
                            else ss.settings.bess_grid_export_net_fixed_megawatt,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                        * (ss.data.market_time_unit_minute * (1.0 / 60.0))
                    ),
                }

                if ss.data.dim_ufi_bess_grid_import is None:
                    del manual_positions_megawatt_hour["Carga Red [MWh]"]

                if ss.data.dim_ufi_bess_res_import is None:
                    del manual_positions_megawatt_hour["Carga Renovable [MWh]"]

                if ss.data.dim_ufi_bess_grid_export is None:
                    del manual_positions_megawatt_hour["Descarga Red [MWh]"]

                ss.manual_positions_megawatt_hour = st.data_editor(
                    manual_positions_megawatt_hour,
                    column_config={"_index": ""},
                    disabled=not ss.run
                    and not ss.manual_positions_megawatt_hour_changed,
                    key=f"manual_positions_megawatt_hour_{ss.manual_positions_megawatt_hour_key}",
                    on_change=_on_change_manual_positions_megawatt_hour,
                )

                st.button(
                    "Borrar",
                    key="manual_positions_megawatt_hour_clear",
                    on_click=_on_change_manual_positions_megawatt_hour,
                    disabled=not ss.run
                    and not ss.manual_positions_megawatt_hour_changed,
                )

                st.button(
                    "Restablecer",
                    key="manual_positions_megawatt_hour_reset",
                    on_click=_on_click_manual_positions_megawatt_hour_reset,
                    disabled=not ss.run
                    and not ss.manual_positions_megawatt_hour_changed,
                )

        (
            auto_state_of_charge_tab,
            manual_state_of_charge_tab,
        ) = st.tabs(["Óptimo", "Manual"])

        with auto_state_of_charge_tab:
            (
                auto_state_of_charge_percent_tab,
                auto_state_of_charge_megawatt_hour_tab,
            ) = st.tabs(["%", "MWh"])

            with auto_state_of_charge_percent_tab:
                auto_initial_state_of_charge_percent = pd.Series(
                    data=ss.data.bess_initial_state_of_charge_percent,
                    index=[""],
                    dtype=float,
                )

                # fmt: off
                auto_state_of_charge_percent = {
                    "Real": (
                        pd.concat(
                            [
                            auto_initial_state_of_charge_percent,
                            ss.data.bess_state_of_charge_megawatt_hour.combine_first(
                                ss.data.bess_actual_state_of_charge_megawatt_hour.fillna(
                                    value=(ss.data.bess_initial_state_of_charge_percent / 100.0)
                                    * ss.data.bess_energy_capacity_megawatt_hour,
                                )
                            )
                            / ss.data.bess_energy_capacity_megawatt_hour
                            * 100.0,
                            ],
                        )
                    ),
                    "Óptimo": (
                        ss.data.bess_state_of_charge_megawatt_hour
                        / ss.data.bess_energy_capacity_megawatt_hour
                        * 100.0
                    ),
                }
                # fmt: on

                auto_state_of_charge_percent_color = {
                    "Real": "#00a44355",
                    "Óptimo": "#00a443aa",
                }

                if ss.data.bess_actual_state_of_charge_megawatt_hour.isna().all():
                    del auto_state_of_charge_percent["Real"]
                    del auto_state_of_charge_percent_color["Real"]

                st.area_chart(
                    data=auto_state_of_charge_percent,
                    y_label="SOC [%]",
                    color=list(auto_state_of_charge_percent_color.values()),
                )

            with auto_state_of_charge_megawatt_hour_tab:
                auto_initial_state_of_charge_megawatt_hour = pd.Series(
                    data=(ss.data.bess_initial_state_of_charge_percent / 100.0)
                    * ss.data.bess_energy_capacity_megawatt_hour,
                    index=[""],
                    dtype=float,
                )

                # fmt: off
                auto_state_of_charge_megawatt_hour = {
                    "Real": (
                        pd.concat(
                            [
                                auto_initial_state_of_charge_megawatt_hour,
                                ss.data.bess_state_of_charge_megawatt_hour.combine_first(
                                    ss.data.bess_actual_state_of_charge_megawatt_hour.fillna(
                                        value=(ss.data.bess_initial_state_of_charge_percent / 100.0)
                                        * ss.data.bess_energy_capacity_megawatt_hour,
                                    )
                                ),
                            ],
                        )
                    ),
                    "Óptimo": ss.data.bess_state_of_charge_megawatt_hour,
                }
                # fmt: on

                auto_state_of_charge_megawatt_hour_color = {
                    "Real": "#00a44355",
                    "Óptimo": "#00a443aa",
                }

                if ss.data.bess_actual_state_of_charge_megawatt_hour.isna().all():
                    del auto_state_of_charge_megawatt_hour["Real"]
                    del auto_state_of_charge_megawatt_hour_color["Real"]

                st.area_chart(
                    data=auto_state_of_charge_megawatt_hour,
                    y_label="SOC [MWh]",
                    color=list(auto_state_of_charge_megawatt_hour_color.values()),
                )

        with manual_state_of_charge_tab:
            (
                manual_state_of_charge_percent_tab,
                manual_state_of_charge_megawatt_hour_tab,
            ) = st.tabs(["%", "MWh"])

            with manual_state_of_charge_percent_tab:
                manual_state_of_charge_percent = {
                    "SOC [%]": (
                        pd.Series(
                            data=ss.data.bess_state_of_charge_fixed_percent
                            if not ss.get("manual_state_of_charge_percent_clear", False)
                            else ss.settings.bess_state_of_charge_fixed_percent,
                            index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                            dtype=float,
                        )
                    ),
                }

                ss.manual_state_of_charge_percent = st.data_editor(
                    manual_state_of_charge_percent,
                    column_config={"_index": ""},
                    disabled=not ss.run
                    and not ss.manual_state_of_charge_percent_changed,
                    key=f"manual_state_of_charge_percent_{ss.manual_state_of_charge_percent_key}",
                    on_change=_on_change_manual_state_of_charge_percent,
                )

                st.button(
                    "Borrar",
                    key="manual_state_of_charge_percent_clear",
                    on_click=_on_change_manual_state_of_charge_percent,
                    disabled=not ss.run
                    and not ss.manual_state_of_charge_percent_changed,
                )

                st.button(
                    "Restablecer",
                    key="manual_state_of_charge_percent_reset",
                    on_click=_on_click_manual_state_of_charge_percent_reset,
                    disabled=not ss.run
                    and not ss.manual_state_of_charge_percent_changed,
                )

            with manual_state_of_charge_megawatt_hour_tab:
                # fmt: off
                manual_state_of_charge_megawatt_hour = {
                    "SOC [MWh]": (
                        (
                            pd.Series(
                                data=ss.data.bess_state_of_charge_fixed_percent
                                if not ss.get("manual_state_of_charge_megawatt_hour_clear", False)
                                else ss.settings.bess_state_of_charge_fixed_percent,
                                index=ss.data.market_price_euro_per_megawatt_hour.dropna().index,
                                dtype=float,
                            )
                            / 100.0
                        )
                        * ss.data.bess_energy_capacity_megawatt_hour
                    ),
                }
                # fmt: on

                ss.manual_state_of_charge_megawatt_hour = st.data_editor(
                    manual_state_of_charge_megawatt_hour,
                    column_config={"_index": ""},
                    disabled=not ss.run
                    and not ss.manual_state_of_charge_megawatt_hour_changed,
                    key=f"manual_state_of_charge_megawatt_hour_{ss.manual_state_of_charge_megawatt_hour_key}",
                    on_change=_on_change_manual_state_of_charge_megawatt_hour,
                )

                st.button(
                    "Borrar",
                    key="manual_state_of_charge_megawatt_hour_clear",
                    on_click=_on_change_manual_state_of_charge_megawatt_hour,
                    disabled=not ss.run
                    and not ss.manual_state_of_charge_megawatt_hour_changed,
                )

                st.button(
                    "Restablecer",
                    key="manual_state_of_charge_megawatt_hour_reset",
                    on_click=_on_click_manual_state_of_charge_megawatt_hour_reset,
                    disabled=not ss.run
                    and not ss.manual_state_of_charge_megawatt_hour_changed,
                )

            _add_vertical_space(num_lines=2)

        # fmt: off
        limits_megawatt = {
            "Batería [MW]": ss.data.bess_grid_export_limits_megawatt.replace(to_replace=math.inf, value=math.nan),
            "Renovable [MW]": ss.data.res_grid_export_limits_megawatt.replace(to_replace=math.inf, value=math.nan),
            "Red [MW]": ss.data.grid_export_limits_megawatt.replace(to_replace=math.inf, value=math.nan),
        }
        # fmt: on

        if ss.data.dim_ufi_bess_grid_export is None:
            del limits_megawatt["Batería [MW]"]

        if ss.data.dim_ufi_bess_res_import is None:
            del limits_megawatt["Renovable [MW]"]

        if any(
            not limit_megawatt.isna().all()
            for limit_megawatt in limits_megawatt.values()
        ):
            st.dataframe(
                limits_megawatt,
                column_config={"_index": "Límites de Exportación"},
            )

    if ss.manual_positions_megawatt_changed:
        # fmt: off
        if ss.data.dim_ufi_bess_grid_import is not None:
            ss.bess_grid_import_net_fixed_megawatt = ss.manual_positions_megawatt["Carga Red [MW]"]
            ss.bess_grid_import_net_fixed_megawatt = ss.bess_grid_import_net_fixed_megawatt.dropna().to_dict()
        else:
            ss.bess_grid_import_net_fixed_megawatt = None
        if ss.data.dim_ufi_bess_res_import is not None:
            ss.bess_res_import_fixed_megawatt = ss.manual_positions_megawatt["Carga Renovable [MW]"]
            ss.bess_res_import_fixed_megawatt = ss.bess_res_import_fixed_megawatt.dropna().to_dict()
        else:
            ss.bess_res_import_fixed_megawatt = None
        if ss.data.dim_ufi_bess_grid_export is not None:
            ss.bess_grid_export_net_fixed_megawatt = ss.manual_positions_megawatt["Descarga Red [MW]"]
            ss.bess_grid_export_net_fixed_megawatt = ss.bess_grid_export_net_fixed_megawatt.dropna().to_dict()
        else:
            ss.bess_grid_export_net_fixed_megawatt = None
    elif ss.manual_positions_megawatt_hour_changed:
        # fmt: off
        if ss.data.dim_ufi_bess_grid_import is not None:
            ss.bess_grid_import_net_fixed_megawatt = ss.manual_positions_megawatt["Carga Red [MWh]"]
            ss.bess_grid_import_net_fixed_megawatt = ss.bess_grid_import_net_fixed_megawatt * ((1.0 / (data.market_time_unit_minute * (1.0 / 60.0))) if data.market_time_unit_minute != 0.0 else 0.0)
            ss.bess_grid_import_net_fixed_megawatt = ss.bess_grid_import_net_fixed_megawatt.dropna().to_dict()
        else:
            ss.bess_grid_import_net_fixed_megawatt = None
        if ss.data.dim_ufi_bess_res_import is not None:
            ss.bess_res_import_fixed_megawatt = ss.manual_positions_megawatt["Carga Renovable [MWh]"]
            ss.bess_res_import_fixed_megawatt = ss.bess_res_import_fixed_megawatt * ((1.0 / (data.market_time_unit_minute * (1.0 / 60.0))) if data.market_time_unit_minute != 0.0 else 0.0)
            ss.bess_res_import_fixed_megawatt = ss.bess_res_import_fixed_megawatt.dropna().to_dict()
        else:
            ss.bess_res_import_fixed_megawatt = None
        if ss.data.dim_ufi_bess_grid_export is not None:
            ss.bess_grid_export_net_fixed_megawatt = ss.manual_positions_megawatt["Descarga Red [MWh]"]
            ss.bess_grid_export_net_fixed_megawatt = ss.bess_grid_export_net_fixed_megawatt * ((1.0 / (data.market_time_unit_minute * (1.0 / 60.0))) if data.market_time_unit_minute != 0.0 else 0.0)
            ss.bess_grid_export_net_fixed_megawatt = ss.bess_grid_export_net_fixed_megawatt.dropna().to_dict()
        else:
            ss.bess_grid_export_net_fixed_megawatt = None
    else:
        ss.bess_grid_import_net_fixed_megawatt = None
        ss.bess_res_import_fixed_megawatt = None
        ss.bess_grid_export_net_fixed_megawatt = None

    if ss.manual_state_of_charge_percent_changed:
        # fmt: off
        ss.bess_state_of_charge_fixed_percent = ss.manual_state_of_charge_percent["SOC [%]"]
        ss.bess_state_of_charge_fixed_percent = ss.bess_state_of_charge_fixed_percent.dropna().to_dict()
    elif ss.manual_state_of_charge_megawatt_hour_changed:
        # fmt: off
        ss.bess_state_of_charge_fixed_percent = ss.manual_state_of_charge_megawatt_hour["SOC [MWh]"]
        ss.bess_state_of_charge_fixed_percent = ss.bess_state_of_charge_fixed_percent / ss.data.bess_energy_capacity_megawatt_hour * 100.0
        ss.bess_state_of_charge_fixed_percent = ss.bess_state_of_charge_fixed_percent.dropna().to_dict()
    else:
        ss.bess_state_of_charge_fixed_percent = None

    ss.manual_positions_megawatt_changed = False
    ss.manual_positions_megawatt_hour_changed = False
    ss.manual_state_of_charge_percent_changed = False
    ss.manual_state_of_charge_megawatt_hour_changed = False

    ss.run = False


def _on_click_run() -> None:
    if (
        ss.bess_grid_import_net_fixed_megawatt is not None
        or ss.bess_res_import_fixed_megawatt is not None
        or ss.bess_grid_export_net_fixed_megawatt is not None
    ):
        # fmt: off
        data = ss.data.copy()
        data.bess_grid_import_net_fixed_megawatt = ss.bess_grid_import_net_fixed_megawatt
        data.bess_res_import_fixed_megawatt = ss.bess_res_import_fixed_megawatt
        data.bess_grid_export_net_fixed_megawatt = ss.bess_grid_export_net_fixed_megawatt
    elif ss.bess_state_of_charge_fixed_percent is not None:
        data = ss.data.copy()
        data.bess_state_of_charge_fixed_percent = ss.bess_state_of_charge_fixed_percent
    else:
        data = Box({key.lower(): value for key, value in ss.settings.as_dict().items()})
    try:
        ss.data = optibat.optibat(data)
    except Exception as e:
        logger.exception(e)
        st.toast(e)
        st.toast("No se pudo ejecutar.", icon="🚨")
    else:
        ss.run = True
        if not ss.data.optimal:
            st.toast("No se pudo encontrar la solución óptima.", icon="⚠️")


def _on_click_save() -> None:
    with FileLock(Path(tempfile.gettempdir(), ".optibat.lock"), timeout=60):
        # fmt: off
        if ss.data.output_XXXX_XXXX_path is not None:
            output_XXXX_XXXX_path = Path(ss.data.output_XXXX_XXXX_path.format(ss.data.current_datetime))
            output_XXXX_XXXX_path.write_text(ss.data.output_XXXX_XXXX, encoding="utf-8", newline="\n")

        if ss.data.output_XXXX_XXXX_path is not None:
            output_XXXX_XXXX_path = Path(ss.data.output_XXXX_XXXX_path.format(ss.data.current_datetime))
            output_XXXX_XXXX_path.write_text(ss.data.output_XXXX_XXXX, encoding="utf-8", newline="\n")

        if ss.data.output_csv_path is not None:
            output_csv_path = Path(ss.data.output_csv_path.format(ss.data.current_datetime))
            output_csv_path.write_text(ss.data.output_csv, encoding="utf-8", newline="\n")

    st.toast("Guardado con éxito.", icon="ℹ️")


def _on_change_module() -> None:
    ss.settings = ss.modules[ss.module]
    ss.reset = True


def _on_click_reset() -> None:
    ss.settings.reload()
    ss.settings.validators.validate_all()
    ss.reset = True


def _on_change_minimum_state_of_charge_percent() -> None:
    if (
        ss.bess_maximum_state_of_charge_percent
        < ss.bess_minimum_state_of_charge_percent
    ):
        ss.bess_maximum_state_of_charge_percent = (
            ss.bess_minimum_state_of_charge_percent
        )

    if (
        ss.bess_initial_state_of_charge_percent is not None
        and ss.bess_initial_state_of_charge_percent
        < ss.bess_minimum_state_of_charge_percent
    ):
        ss.bess_initial_state_of_charge_percent = (
            ss.bess_minimum_state_of_charge_percent
        )

    if (
        ss.bess_final_state_of_charge_percent is not None
        and ss.bess_final_state_of_charge_percent
        < ss.bess_minimum_state_of_charge_percent
    ):
        ss.bess_final_state_of_charge_percent = ss.bess_minimum_state_of_charge_percent


def _on_change_maximum_state_of_charge_percent() -> None:
    if (
        ss.bess_minimum_state_of_charge_percent
        > ss.bess_maximum_state_of_charge_percent
    ):
        ss.bess_minimum_state_of_charge_percent = (
            ss.bess_maximum_state_of_charge_percent
        )

    if (
        ss.bess_initial_state_of_charge_percent is not None
        and ss.bess_initial_state_of_charge_percent
        > ss.bess_maximum_state_of_charge_percent
    ):
        ss.bess_initial_state_of_charge_percent = (
            ss.bess_maximum_state_of_charge_percent
        )

    if (
        ss.bess_final_state_of_charge_percent is not None
        and ss.bess_final_state_of_charge_percent
        > ss.bess_maximum_state_of_charge_percent
    ):
        ss.bess_final_state_of_charge_percent = ss.bess_maximum_state_of_charge_percent


def _on_change_input_csv() -> None:
    ss.market_csv = ss.input_csv


def _on_change_auto_enabled():
    with FileLock(ss.settings.path_for("config.local.yaml.lock"), timeout=60):
        loader.write(
            ss.settings.path_for("config.local.yaml"),
            {"default": {"auto_enabled": ss.auto_enabled}},
        )


def _on_change_manual_positions_megawatt() -> None:
    ss.manual_positions_megawatt_changed = True


def _on_click_manual_positions_megawatt_reset() -> None:
    ss.manual_positions_megawatt_key = uuid.uuid4().hex
    ss.run = True


def _on_change_manual_positions_megawatt_hour() -> None:
    ss.manual_positions_megawatt_hour_changed = True


def _on_click_manual_positions_megawatt_hour_reset() -> None:
    ss.manual_positions_megawatt_hour_key = uuid.uuid4().hex
    ss.run = True


def _on_change_manual_state_of_charge_percent() -> None:
    ss.manual_state_of_charge_percent_changed = True


def _on_click_manual_state_of_charge_percent_reset() -> None:
    ss.manual_state_of_charge_percent_key = uuid.uuid4().hex
    ss.run = True


def _on_change_manual_state_of_charge_megawatt_hour() -> None:
    ss.manual_state_of_charge_megawatt_hour_changed = True


def _on_click_manual_state_of_charge_megawatt_hour_reset() -> None:
    ss.manual_state_of_charge_megawatt_hour_key = uuid.uuid4().hex
    ss.run = True


def _read_main_path() -> ContextManager[Path]:
    main_path = as_file(files("optibat").joinpath("__main__.py"))
    return main_path


def _read_favicon_path() -> ContextManager[Path]:
    favicon_path = as_file(files("optibat").joinpath("static/favicon.ico"))
    return favicon_path


def _read_icon_path() -> ContextManager[Path]:
    icon_path = as_file(files("optibat").joinpath("static/icon.png"))
    return icon_path


def _read_logo_path() -> ContextManager[Path]:
    logo_path = as_file(files("optibat").joinpath("static/logo.svg"))
    return logo_path


def _add_vertical_space(num_lines: int = 1) -> None:
    for _ in range(num_lines):
        st.write("")


if __name__ == "__main__":
    main()
