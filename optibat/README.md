# optibat

> Cross-market battery optimization

> [!NOTE]
> Consultar el documento XXXX_XXXX y el vídeo formativo en XXXX_XXXX para más información (en especial interés para XXXX_XXXX).

## Configuración

| Parámetro                                      | Tipo         | Descripción                                                                                                 |
|:-----------------------------------------------|:-------------|:------------------------------------------------------------------------------------------------------------|
| headless                                       | bool         | Ejecutar en modo no automático sin panel de control.                                                        |
| current_datetime                               | datetime/null| Fecha y hora actual para simulaciones o pruebas. Si es null, se usa la del sistema.                         |
| market_date                                    | date/null    | Fecha de mercado objetivo. Si es null, se calcula automáticamente según el tipo de mercado.                 |
| market_timezone                                | str          | Zona horaria para todas las operaciones de fecha/hora (por defecto Europe/Madrid).                          |
| market_type                                    | str          | Tipo de mercado: MD (Diario), MI1, MI2, MI3 (Intradiarios), MIC (Continuo).                                 |
| market_horizon_day                             | int          | Días a optimizar o prever.                                                                                  |
| market_history_day                             | int          | Días de histórico a usar para cálculos y validaciones.                                                      |
| market_forecast                                | str          | Identificador del escenario de previsión.                                                                   |
| market_csv                                     | str/null     | Ruta a CSV para datos de mercado offline (null para usar base de datos).                                    |
| market_rate                                    | float        | Parámetro de priorización de posiciones actuales (ajusta la preferencia por mantener posiciones).           |
| market_time_unit_minute                        | int          | Unidad temporal del mercado en minutos (MTU).                                                               |
| dim_ufi_bess_grid_import                       | str/null     | UFI para importación de red a batería.                                                                      |
| dim_ufi_bess_res_import                        | str/null     | UFI para importación renovable a batería.                                                                   |
| dim_ufi_bess_grid_export                       | str/null     | UFI para exportación de batería a red.                                                                      |
| dim_ufi_bess_charge                            | str/null     | UFI para carga de batería.                                                                                  |
| dim_ufi_res_grid_export                        | str/null     | UFI para exportación renovable a red.                                                                       |
| dim_up_grid_export                             | str/null     | UFI para exportación de unidad de producción a red.                                                         |
| dim_state_of_charge_point                      | str/null     | Punto PI para estado de carga de la batería.                                                                |
| dim_state_of_health_point                      | str/null     | Punto PI para estado de salud de la batería.                                                                |
| dim_charging_power_capacity_point              | str/null     | Punto PI para capacidad de carga de la batería.                                                             |
| dim_discharging_power_capacity_point           | str/null     | Punto PI para capacidad de descarga de la batería.                                                          |
| dim_availability_point                         | str/null     | Punto PI para disponibilidad de la batería.                                                                 |
| dim_charging_efficiency_point                  | str/null     | Punto PI para eficiencia de carga.                                                                          |
| dim_discharging_efficiency_point               | str/null     | Punto PI para eficiencia de descarga.                                                                       |
| dim_program_point                              | str/null     | Punto PI para consignas/programación.                                                                       |
| bess_power_capacity_megawatt                   | float        | Potencia máxima de la batería (MW).                                                                         |
| bess_energy_capacity_megawatt_hour             | float        | Energía máxima de la batería (MWh).                                                                         |
| bess_charging_efficiency_percent               | float        | Eficiencia de carga de la batería (%).                                                                      |
| bess_discharging_efficiency_percent            | float        | Eficiencia de descarga de la batería (%).                                                                   |
| bess_maximum_cycles_count_per_day              | float        | Ciclos máximos permitidos por día.                                                                          |
| bess_profit_threshold_euro_per_megawatt_hour   | float        | Umbral de beneficio objetivo (€/MWh).                                                                       |
| bess_minimum_state_of_charge_percent           | float        | Estado de carga mínimo permitido (%).                                                                       |
| bess_maximum_state_of_charge_percent           | float        | Estado de carga máximo permitido (%).                                                                       |
| bess_initial_state_of_charge_percent           | float/null   | Estado de carga inicial (%). Si es null, se calcula automáticamente.                                        |
| bess_final_state_of_charge_percent             | float/null   | Estado de carga final objetivo (%).                                                                         |
| bess_availability_percent                      | float/null   | Disponibilidad de la batería (%).                                                                           |
| bess_res_import_clipping_percent               | float        | Límite de clipping para importación renovable (%).                                                          |
| bess_res_import_clipping_threshold_megawatt    | float        | Umbral de clipping para importación renovable (MW).                                                         |
| bess_res_import_priority                       | bool         | Prioridad de importación renovable (True: prioridad renovable, False: prioridad red).                       |
| bess_state_of_charge_tolerance_percent         | float        | Tolerancia para el estado de carga (%).                                                                     |
| bess_purchase_tolerance_euro_per_megawatt_hour | float        | Tolerancia de compra (€/MWh).                                                                               |
| bess_sale_tolerance_euro_per_megawatt_hour     | float        | Tolerancia de venta (€/MWh).                                                                                |
| bess_grid_import_net_fixed_megawatt            | dict         | Programación fija de importación de red (para simulaciones).                                                |
| bess_res_import_fixed_megawatt                 | dict         | Programación fija de importación renovable (para simulaciones).                                             |
| bess_grid_export_net_fixed_megawatt            | dict         | Programación fija de exportación de red (para simulaciones).                                                |
| bess_state_of_charge_fixed_percent             | dict         | Estado de carga fijo por periodo (para simulaciones).                                                       |
| res_export_price_euro_per_megawatt_hour        | float/null   | Precio de exportación renovable (€/MWh).                                                                    |
| grid_export_limit_megawatt                     | float        | Límite de exportación a red (MW).                                                                           |
| solver                                         | str          | Solucionador de optimización (glpk, cbc, etc.).                                                             |
| output_csv_path                                | str/null     | Ruta para salida CSV de resultados de mercado.                                                              |
| output_XXXX_XXXX_path                          | str/null     | Ruta para salida de ofertas para XXXX_XXXX.                                                                 |
| output_XXXX_XXXX_path                          | str/null     | Ruta para salida de ofertas para XXXX_XXXX.                                                                 |
| output_block                                   | int          | Identificador de bloque de salida (normalmente 1).                                                          |
| auto_enabled                                   | bool         | Habilita la ejecución automática en producción.                                                             |
| modules                                        | list         | Lista de instalaciones o módulos configurados.                                                              |
| auth                                           | dict         | Credenciales para autenticación (nombre de usuario, etc.).                                                  |
| market                                         | dict         | Credenciales y datos de conexión a base de datos de mercado.                                                |
| metering                                       | dict         | Credenciales y datos de conexión a PI System.                                                               |

En cada sección (`XXXX_XXXX`, `XXXX_XXXX`, `XXXX_XXXX`, ...) se pueden sobrescribir los parámetros de la sección `default` por defecto para una instalación o escenario concreto.

## Authors

- Josu Gomez Arana (XXXX_XXXX) ~~<XXXX_XXXX@XXXX_XXXX>~~ - Developer and Infrastructure
- XXXX_XXXX - Maintainers
- XXXX_XXXX XXXX_XXXX XXXX_XXXX (XXXX_XXXX) <XXXX_XXXX@XXXX_XXXX> - Supervisor
- XXXX_XXXX XXXX_XXXX XXXX_XXXX (XXXX_XXXX) <XXXX_XXXX@XXXX_XXXX> - Project Manager
