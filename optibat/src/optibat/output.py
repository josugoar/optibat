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


def _to_module(data: Box) -> str | None:
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
    if data.output_XXXX_XXXX_path is None:
        output_XXXX_XXXX = None
        return output_XXXX_XXXX

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

    output_XXXX_XXXX = pd.concat(
        [
            bess_grid_import_output_XXXX_XXXX,
            bess_res_import_output_XXXX_XXXX,
            bess_grid_export_output_XXXX_XXXX,
            bess_charge_output_XXXX_XXXX,
        ],
    )

    output_XXXX_XXXX = output_XXXX_XXXX.dropna()

    output_XXXX_XXXX["ENERGIA"] = output_XXXX_XXXX["ENERGIA"].round(decimals=1)
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


def _to_XXXX_XXXX(data: Box) -> str | None:
    if data.output_XXXX_XXXX_path is None:
        output_XXXX_XXXX = None
        return output_XXXX_XXXX

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
