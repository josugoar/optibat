"""
Output generation for setpoints and offers.

It serializes and exports results from the optimization pipeline to various formats,
including PI System modules, custom local files and XXXX_XXXX specific formats for bidding with XXXX_XXXX.
It ensures outputs are consistent, well formatted and ready for integration with downstream
systems or regulatory reporting.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

import tempfile
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from box import Box
from filelock import FileLock

from optibat.metering import write_module


def write_output(data: Box) -> Box:
    """
    Generate and export all configured outputs.

    This function writes results to PI System modules, custom local files, and market specific templates
    as required by the configuration. File locking is used in headless mode to prevent concurrent
    writes.

    Args:
        data (Box): Input data and configuration, including output paths and flags.

    Returns:
        Box: The merged data and output references.
    """
    # MUST lock file before writing, otherwise XXXX_XXXX WILL have conflicts and bids will be lost,
    # while the battery still cycles, wasting money and energy.
    with (
        FileLock(Path(tempfile.gettempdir(), ".optibat.lock"), timeout=60)
        if data.headless
        else nullcontext()
    ):
        _to_module(data)
        output_XXXX_XXXX = _to_XXXX_XXXX(data)
        output_XXXX_XXXX = _to_XXXX_XXXX(data)
        output_csv = _to_csv(data)
        output = Box(
            output_XXXX_XXXX=output_XXXX_XXXX,
            output_XXXX_XXXX=output_XXXX_XXXX,
            output_csv=output_csv,
        )
        return data | output


def _to_module(data: Box) -> None:
    """
    Write BESS charge and discharge results to the PI System.

    This function prepares and formats the output DataFrame, rounds values for reporting,
    and delegates the actual write to the metering integration.
    """
    # Not everything is actually needed.
    bess_charge_output_module = pd.DataFrame(
        data={
            "FECHA": data.market_dates,
            "PERIODO": data.market_periods,
            "COD_RESOLUCION": f"{data.market_time_unit_minute}M",
            "BLOQUE": data.output_block,
            "UFI": data.dim_ufi_bess_charge,
            "ENERGIA": (
                data.bess_discharge_megawatt_hour - data.bess_charge_megawatt_hour
            ),
            "POTENCIA": (
                (data.bess_discharge_megawatt_hour - data.bess_charge_megawatt_hour)
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
        }
    )

    output_module = output_module.dropna()

    output_module["ENERGIA"] = output_module["ENERGIA"].round(decimals=1)
    output_module["POTENCIA"] = output_module["POTENCIA"].round(decimals=1)
    output_module["PRECIO"] = output_module["PRECIO"].round(decimals=2)

    write_module(data, output_module)


def _to_XXXX_XXXX(data: Box) -> str | None:
    """
    Export results to a XXXX_XXXX.

    This function prepares and formats the output DataFrame for market integration,
    rounds values, and writes to the configured path if in headless mode. Returns the data
    to be given XXXX_XXXX.
    """
    if data.output_XXXX_XXXX_path is None:
        output_XXXX_XXXX = None
        return output_XXXX_XXXX

    # XXXX_XXXX DOES NOT KNOW ABOUT RECOMPRAS Y REVENTAS! For that use XXXX_XXXX when
    # XXXX_XXXX is ready.

    bess_grid_import_output_XXXX_XXXX = pd.DataFrame(
        data={
            "FECHA": data.market_dates,
            "PERIODO": data.market_periods,
            "COD_RESOLUCION": f"{data.market_time_unit_minute}M",
            "BLOQUE": data.output_block,
            "UFI": data.dim_ufi_bess_grid_import,
            "ENERGIA": data.bess_grid_import_net_megawatt_hour,
            "POTENCIA": (
                data.bess_grid_import_net_megawatt_hour
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
        }
    )

    bess_res_import_output_XXXX_XXXX = pd.DataFrame(
        data={
            "FECHA": data.market_dates,
            "PERIODO": data.market_periods,
            "COD_RESOLUCION": f"{data.market_time_unit_minute}M",
            "BLOQUE": data.output_block,
            "UFI": data.dim_ufi_res_grid_export,
            "ENERGIA": -data.bess_res_import_megawatt_hour,
            "POTENCIA": (
                -data.bess_res_import_megawatt_hour
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
        }
    )

    bess_grid_export_output_XXXX_XXXX = pd.DataFrame(
        data={
            "FECHA": data.market_dates,
            "PERIODO": data.market_periods,
            "COD_RESOLUCION": f"{data.market_time_unit_minute}M",
            "BLOQUE": data.output_block,
            "UFI": data.dim_ufi_bess_grid_export,
            "ENERGIA": data.bess_grid_export_net_megawatt_hour,
            "POTENCIA": (
                data.bess_grid_export_net_megawatt_hour
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
        }
    )

    bess_charge_output_XXXX_XXXX = pd.DataFrame(
        data={
            "FECHA": data.market_dates,
            "PERIODO": data.market_periods,
            "COD_RESOLUCION": f"{data.market_time_unit_minute}M",
            "BLOQUE": data.output_block,
            "UFI": data.dim_ufi_bess_charge,
            "ENERGIA": (
                data.bess_discharge_megawatt_hour - data.bess_charge_megawatt_hour
            ),
            "POTENCIA": (
                (data.bess_discharge_megawatt_hour - data.bess_charge_megawatt_hour)
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
        }
    )

    # Recommended to bunch all UFIs together, so that XXXX_XXXX distinguishes between
    # runs instead of the UFIs themselves.
    output_XXXX_XXXX = pd.concat(
        [
            bess_grid_import_output_XXXX_XXXX,
            bess_res_import_output_XXXX_XXXX,
            bess_grid_export_output_XXXX_XXXX,
            bess_charge_output_XXXX_XXXX,
        ],
    )

    # DO NOT erase previouse results.
    output_XXXX_XXXX = output_XXXX_XXXX.dropna()

    # Rounding values will generate desvíos, but they are so small that XXXX_XXXX will
    # supposedly sort them out. The resolution is the official one anyway, so it's fine.
    output_XXXX_XXXX["ENERGIA"] = output_XXXX_XXXX["ENERGIA"].round(decimals=1)
    output_XXXX_XXXX["POTENCIA"] = output_XXXX_XXXX["POTENCIA"].round(decimals=1)
    output_XXXX_XXXX["PRECIO"] = output_XXXX_XXXX["PRECIO"].round(decimals=2)

    # If the format changes, consult with XXXX_XXXX.
    output_XXXX_XXXX = output_XXXX_XXXX.to_csv(
        path_or_buf=data.output_XXXX_XXXX_path.format(datetime.now(tz=ZoneInfo(data.market_timezone)))
        if data.headless
        else None,
        sep=";",
        index=False,
        encoding="utf-8",
        lineterminator="\n",
        date_format="%d/%m/%Y",
    )

    return output_XXXX_XXXX


def _to_XXXX_XXXX(data: Box) -> str | None:
    if data.output_XXXX_XXXX_path is None:
        output_XXXX_XXXX = None
        return output_XXXX_XXXX

    # Similar to XXXX_XXXX data, BUT IT SUPPORTS RECOMPRAS Y REVENTAS, so the
    # direction (TIPO_OFERTA) FOR EACH UFI is important.

    bess_grid_import_output_XXXX_XXXX = pd.DataFrame(
        data={
            "COD_ENTIDAD": data.dim_ufi_bess_grid_import,
            "FEC_MERCADO": data.market_dates,
            "COD_MERCADO": data.market_types,
            "SESION": data.market_sessions,
            "BLOQUE": data.output_block,
            "POTENCIA": (
                data.bess_grid_import_gross_megawatt_hour.abs()
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
            "TIPO_OFERTA": (
                data.bess_grid_import_gross_megawatt_hour.case_when(
                    [
                        (data.bess_grid_import_gross_megawatt_hour > 0.0, "C"),
                        (data.bess_grid_import_gross_megawatt_hour < 0.0, "V"),
                        (data.bess_grid_import_gross_megawatt_hour == 0.0, None),
                    ]
                )
            ),
            "FEC_PROGRAMACION": data.market_dates,
            "ID_QH": data.market_periods,
        }
    )

    bess_grid_export_output_XXXX_XXXX = pd.DataFrame(
        data={
            "COD_ENTIDAD": data.dim_ufi_bess_grid_export,
            "FEC_MERCADO": data.market_dates,
            "COD_MERCADO": data.market_types,
            "SESION": data.market_sessions,
            "BLOQUE": data.output_block,
            "POTENCIA": (
                data.bess_grid_export_gross_megawatt_hour.abs()
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.bess_price_euro_per_megawatt_hour,
            "TIPO_OFERTA": (
                data.bess_grid_export_gross_megawatt_hour.case_when(
                    [
                        (data.bess_grid_export_gross_megawatt_hour < 0.0, "C"),
                        (data.bess_grid_export_gross_megawatt_hour > 0.0, "V"),
                        (data.bess_grid_export_gross_megawatt_hour == 0.0, None),
                    ]
                )
            ),
            "FEC_PROGRAMACION": data.market_dates,
            "ID_QH": data.market_periods,
        }
    )

    res_grid_export_output_XXXX_XXXX = pd.DataFrame(
        data={
            "COD_ENTIDAD": data.dim_ufi_res_grid_export,
            "FEC_MERCADO": data.market_dates,
            "COD_MERCADO": data.market_types,
            "SESION": data.market_sessions,
            "BLOQUE": data.output_block,
            "POTENCIA": (
                data.res_grid_export_gross_megawatt_hour.abs()
                * (1.0 / (data.market_time_unit_minute * (1.0 / 60.0)))
                if data.market_time_unit_minute != 0.0
                else 0.0
            ),
            "PRECIO": data.res_price_euro_per_megawatt_hour,
            "TIPO_OFERTA": (
                data.res_grid_export_gross_megawatt_hour.case_when(
                    [
                        (data.res_grid_export_gross_megawatt_hour < 0.0, "C"),
                        (data.res_grid_export_gross_megawatt_hour > 0.0, "V"),
                        (data.res_grid_export_gross_megawatt_hour == 0.0, None),
                    ]
                )
            ),
            "FEC_PROGRAMACION": data.market_dates,
            "ID_QH": data.market_periods,
        }
    )

    output_XXXX_XXXX = pd.concat(
        [
            bess_grid_import_output_XXXX_XXXX,
            bess_grid_export_output_XXXX_XXXX,
            res_grid_export_output_XXXX_XXXX,
        ],
    )

    output_XXXX_XXXX = output_XXXX_XXXX.dropna()

    output_XXXX_XXXX["POTENCIA"] = output_XXXX_XXXX["POTENCIA"].round(decimals=1)
    output_XXXX_XXXX["PRECIO"] = output_XXXX_XXXX["PRECIO"].round(decimals=2)

    output_XXXX_XXXX = output_XXXX_XXXX.to_csv(
        path_or_buf=data.output_XXXX_XXXX_path.format(datetime.now(tz=ZoneInfo(data.market_timezone)))
        if data.headless
        else None,
        sep=";",
        index=False,
        encoding="utf-8",
        lineterminator="\n",
        date_format="%d/%m/%Y",
    )

    return output_XXXX_XXXX


def _to_csv(data: Box) -> str | None:
    """
    Export the raw market input to a local CSV file for download.

    This function writes the market input DataFrame to the configured path if
    in headless mode, or returns the CSV string for further processing. It is
    useful for drilling down the results.
    """
    if data.output_csv_path is None:
        output_csv = None
        return output_csv

    output_csv = data.market_input.to_csv(
        path_or_buf=data.output_csv_path.format(datetime.now(tz=ZoneInfo(data.market_timezone)))
        if data.headless
        else None,
        sep=";",
        index=True,
        encoding="utf-8",
        lineterminator="\n",
        date_format="%d/%m/%Y",
    )

    return output_csv
