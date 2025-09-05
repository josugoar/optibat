with market as (
   -- Generate a series of the entire optimization horizon taking daylight saving
   -- into account and having both the date and the period identifier, so that
   -- it's easier to integrate them with XXXX_XXXX.
   select trunc(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') + (level - 1) * interval '15' minute, 'DD') as market_dates,
          extract(day from 96 * (from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') + level * interval '15' minute - from_tz(cast(trunc(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') + (level - 1) * interval '15' minute, 'DD') as timestamp), 'Europe/Madrid'))) as market_periods
     from dual
  connect by
     level <= extract(day from 96 * (from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')))
), XXXX_XXXX as (
   -- Quite slow because XXXX_XXXX does not have indices...
   select XXXX_XXXX.fecha as market_dates,
          XXXX_XXXX.hora as market_periods,
          case :market_type when 'MIC' then 'MIC' else 'MD' end as market_types,
          case :market_type when 'MIC' then extract(hour from from_tz(cast(:market_datetime as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')) + 1 else 0 end as market_sessions,
          any_value(XXXX_XXXX.precio_final) keep(dense_rank first order by XXXX_XXXX.ult_f_ejec desc nulls last) as price_euro_per_megawatt_hour
     from XXXX_XXXX XXXX_XXXX
    where XXXX_XXXX.fecha >= trunc(:market_datetime, 'DD')
      and XXXX_XXXX.fecha < trunc(:market_datetime, 'DD') + :market_horizon_day
      and XXXX_XXXX.ult_f_ejec > :market_datetime - :market_history_day * interval '1' day
      and XXXX_XXXX.ult_f_ejec <= :market_datetime
    group by XXXX_XXXX.fecha,
             XXXX_XXXX.hora
), XXXX_XXXX as (
   select XXXX_XXXX.fecha as market_dates,
          XXXX_XXXX.periodo as market_periods,
          case :market_type when 'MIC' then 'MIC' else 'MD' end as market_types,
          case :market_type when 'MIC' then extract(hour from from_tz(cast(:market_datetime as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')) + 1 else 0 end as market_sessions,
          any_value(XXXX_XXXX.valor) keep(dense_rank first order by fc.version desc nulls last) as price_euro_per_megawatt_hour
     from XXXX_XXXX XXXX_XXXX
    --- Interesting trick to join with XXXX_XXXX to improve performance.
    inner join XXXX_XXXX fc
       on XXXX_XXXX.fk_id_fichero_cargado = fc.id_fichero_cargado
    where XXXX_XXXX.fk_id_descriptor in (595, 596)
      and XXXX_XXXX.fecha >= trunc(:market_datetime, 'DD')
      and XXXX_XXXX.fecha < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and (fc.nombre like 'XXXX_XXXX%' or fc.nombre like 'XXXX_XXXX%')
      and fc.fk_id_fichero in (XXXX_XXXX, XXXX_XXXX)
      and fc.fecha <= trunc(:market_datetime, 'DD')
      and fc.fecha_insercion > :market_datetime - :market_history_day * interval '1' day
      and fc.fecha_insercion <= :market_datetime
    group by XXXX_XXXX.fecha,
             XXXX_XXXX.periodo
), pdbc as (
   select pdbc.fec_pdbc as market_dates,
          round((pdbc.hora_ini_utc - cast(from_tz(cast(pdbc.fec_pdbc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pdbc.fec_pdbc + interval '1' hour - pdbc.fec_pdbc) + 1) as market_periods,
          case :market_type when 'MIC' then 'MIC' else 'MI' end as market_types,
          case :market_type when 'MIC' then extract(hour from from_tz(cast(:market_datetime as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')) + 1 else 1 end as market_sessions,
          any_value(pdbc.precio_marg) keep(dense_rank first order by pdbc.version desc nulls last) as price_euro_per_megawatt_hour
     from XXXX_XXXX pdbc
    where pdbc.cod_pais = 'ES'
      and pdbc.fec_version > :market_datetime - :market_history_day * interval '1' day
      and pdbc.fec_version <= :market_datetime
      and pdbc.fec_pdbc >= trunc(:market_datetime, 'DD')
      and pdbc.fec_pdbc < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pdbc.hora_ini_utc >= cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pdbc.hora_ini_utc < cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      -- WHEN DAILY MARKET CHANGES TO CUARTOHORARIO, CHANGE THIS TO PT15M.
      and pdbc.resolucion = 'PT60M'
    group by pdbc.fec_pdbc,
             round((pdbc.hora_ini_utc - cast(from_tz(cast(pdbc.fec_pdbc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pdbc.fec_pdbc + interval '1' hour - pdbc.fec_pdbc) + 1)
-- Query each one separately to improve performance and distinguish their prices for the forecast.
), pibc1 as (
   select pibc1.fec_pibc as market_dates,
          round((pibc1.hora_ini_utc - cast(from_tz(cast(pibc1.fec_pibc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pibc1.fec_pibc + interval '15' minute - pibc1.fec_pibc) + 1) as market_periods,
          case :market_type when 'MIC' then 'MIC' else 'MI' end as market_types,
          case :market_type when 'MIC' then extract(hour from from_tz(cast(:market_datetime as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')) + 1 else 2 end as market_sessions,
          any_value(pibc1.precio_marg) keep(dense_rank first order by pibc1.version desc nulls last) as price_euro_per_megawatt_hour
     from XXXX_XXXX pibc1
    where pibc1.cod_pais = 'ES'
      and pibc1.fec_version > :market_datetime - :market_history_day * interval '1' day
      and pibc1.fec_version <= :market_datetime
      and pibc1.fec_pibc >= trunc(:market_datetime, 'DD')
      and pibc1.fec_pibc < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pibc1.hora_ini_utc >= cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pibc1.hora_ini_utc < cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pibc1.num_sesion = 1
      and pibc1.resolucion = 'PT15M'
    group by pibc1.fec_pibc,
          round((pibc1.hora_ini_utc - cast(from_tz(cast(pibc1.fec_pibc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pibc1.fec_pibc + interval '15' minute - pibc1.fec_pibc) + 1)
), pibc2 as (
   select pibc2.fec_pibc as market_dates,
          round((pibc2.hora_ini_utc - cast(from_tz(cast(pibc2.fec_pibc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pibc2.fec_pibc + interval '15' minute - pibc2.fec_pibc) + 1) as market_periods,
          case :market_type when 'MIC' then 'MIC' else 'MI' end as market_types,
          case :market_type when 'MIC' then extract(hour from from_tz(cast(:market_datetime as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')) + 1 else 3 end as market_sessions,
          any_value(pibc2.precio_marg) keep(dense_rank first order by pibc2.version desc nulls last) as price_euro_per_megawatt_hour
     from XXXX_XXXX pibc2
    where pibc2.cod_pais = 'ES'
      and pibc2.fec_version > :market_datetime - :market_history_day * interval '1' day
      and pibc2.fec_version <= :market_datetime
      and pibc2.fec_pibc >= trunc(:market_datetime, 'DD')
      and pibc2.fec_pibc < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pibc2.hora_ini_utc >= cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pibc2.hora_ini_utc < cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pibc2.num_sesion = 2
      and pibc2.resolucion = 'PT15M'
    group by pibc2.fec_pibc,
             round((pibc2.hora_ini_utc - cast(from_tz(cast(pibc2.fec_pibc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pibc2.fec_pibc + interval '15' minute - pibc2.fec_pibc) + 1)
), pibc3 as (
   select pibc3.fec_pibc as market_dates,
          case :market_type when 'MIC' then 'MIC' else null end as market_types,
          case :market_type when 'MIC' then extract(hour from from_tz(cast(:market_datetime as timestamp), 'Europe/Madrid') - from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid')) + 1 else null end as market_sessions,
          round((pibc3.hora_ini_utc - cast(from_tz(cast(pibc3.fec_pibc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pibc3.fec_pibc + interval '15' minute - pibc3.fec_pibc) + 1) as market_periods,
          any_value(pibc3.precio_marg) keep(dense_rank first order by pibc3.version desc nulls last) as price_euro_per_megawatt_hour
     from XXXX_XXXX pibc3
    where pibc3.cod_pais = 'ES'
      and pibc3.fec_version > :market_datetime - :market_history_day * interval '1' day
      and pibc3.fec_version <= :market_datetime
      and pibc3.fec_pibc >= trunc(:market_datetime, 'DD')
      and pibc3.fec_pibc < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pibc3.hora_ini_utc >= cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pibc3.hora_ini_utc < cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and pibc3.num_sesion = 3
      and pibc3.resolucion = 'PT15M'
    group by pibc3.fec_pibc,
             round((pibc3.hora_ini_utc - cast(from_tz(cast(pibc3.fec_pibc as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)) / (pibc3.fec_pibc + interval '15' minute - pibc3.fec_pibc) + 1)
), prices as (
   select market.market_dates,
          market.market_periods,
          -- Price priority scheme. For each market, take the latest price available and treat it as the forecast.
          coalesce(pibc3.market_types, pibc2.market_types, pibc1.market_types, pdbc.market_types, XXXX_XXXX.market_types, XXXX_XXXX.market_types) as market_types,
          coalesce(pibc3.market_sessions, pibc2.market_sessions, pibc1.market_sessions, pdbc.market_sessions, XXXX_XXXX.market_sessions, XXXX_XXXX.market_sessions) as market_sessions,
          coalesce(pibc3.price_euro_per_megawatt_hour, pibc2.price_euro_per_megawatt_hour, pibc1.price_euro_per_megawatt_hour, pdbc.price_euro_per_megawatt_hour, XXXX_XXXX.price_euro_per_megawatt_hour, XXXX_XXXX.price_euro_per_megawatt_hour) as price_euro_per_megawatt_hour
     from market
     left join XXXX_XXXX
       on market.market_dates = XXXX_XXXX.market_dates
      and ceil(market.market_periods / 4) = XXXX_XXXX.market_periods
      and :market_type in ('MD', 'MI1', 'MI2', 'MI3', 'MIC')
      and :market_forecast = 'XXXX_XXXX'
     left join XXXX_XXXX
       on market.market_dates = XXXX_XXXX.market_dates
      and ceil(market.market_periods / 4) = XXXX_XXXX.market_periods
      and :market_type in ('MD', 'MI1', 'MI2', 'MI3', 'MIC')
      and :market_forecast = 'XXXX_XXXX'
     left join pdbc
       on market.market_dates = pdbc.market_dates
      and ceil(market.market_periods / 4) = pdbc.market_periods
      and :market_type in ('MI1', 'MI2', 'MI3', 'MIC')
     left join pibc1
       on market.market_dates = pibc1.market_dates
      and market.market_periods = pibc1.market_periods
      and :market_type in ('MI2', 'MI3', 'MIC')
     left join pibc2
       on market.market_dates = pibc2.market_dates
      and market.market_periods = pibc2.market_periods
      and :market_type in ('MI3', 'MIC')
     left join pibc3
       on market.market_dates = pibc3.market_dates
      and market.market_periods = pibc3.market_periods
      and :market_type in ('MIC')
), energies as (
   select trunc(energies.fechalocal, 'DD') as market_dates,
          energies.periodolocal as market_periods,
          any_value(energies.energia) keep(dense_rank first order by energies.fechapublicacion desc nulls last) as energy_megawatt_hour
     from XXXX_XXXX energies
    where energies.cdcilxxx = :dim_ufi_res_grid_export
      -- MASSIVE performance improvement with fechapublicacion, entire bottleneck reduced.
      and energies.fechapublicacion > :market_datetime - :market_history_day * interval '1' day
      and energies.fechapublicacion <= :market_datetime
      and energies.fechainicio >= cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and energies.fechainicio < cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and energies.fechafin > cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and energies.fechafin <= cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and trunc(energies.fechalocal, 'DD') >= trunc(:market_datetime, 'DD')
      and trunc(energies.fechalocal, 'DD') < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
    group by trunc(energies.fechalocal, 'DD'),
             energies.periodolocal
), pdbf as (
   select pdbf.fec_mercado as market_dates,
          pdbf.id_hora as market_periods,
          pdbf.cod_entidad as ufi,
          abs(any_value(pdbf.valor_magnitud) keep(dense_rank first order by pdbf.version desc nulls last)) / 4 as position_megawatt_hour
     from XXXX_XXXX pdbf
    where pdbf.cod_mercado = 'ND_MD'
      and pdbf.num_sesion = '0'
      and pdbf.cod_entidad in (:dim_ufi_bess_grid_import, :dim_ufi_bess_grid_export, :dim_ufi_res_grid_export)
      and pdbf.tipo_entidad = 'UFI'
      and pdbf.fec_mercado >= trunc(:market_datetime, 'DD')
      and pdbf.fec_mercado < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pdbf.tipo_oferta = 'DESG_MD'
      and pdbf.cod_pais = 'ESPAÑA'
      and pdbf.resolucion = 'PT15M'
      and pdbf.fec_generacion > :market_datetime - :market_history_day * interval '1' day
      and pdbf.fec_generacion <= :market_datetime
    group by pdbf.fec_mercado,
             pdbf.id_hora,
             pdbf.cod_entidad
), pibca1 as (
   select pibca1.fec_mercado as market_dates,
          pibca1.id_hora as market_periods,
          pibca1.cod_entidad as ufi,
          abs(any_value(pibca1.valor_magnitud) keep(dense_rank first order by pibca1.version desc nulls last)) / 4 as position_megawatt_hour
     from XXXX_XXXX pibca1
    where pibca1.cod_mercado = 'D_MI'
      and pibca1.num_sesion = '1'
      and pibca1.cod_entidad in (:dim_ufi_bess_grid_import, :dim_ufi_bess_grid_export, :dim_ufi_res_grid_export)
      and pibca1.tipo_entidad = 'UFI'
      and pibca1.fec_mercado >= trunc(:market_datetime, 'DD')
      and pibca1.fec_mercado < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pibca1.tipo_oferta = 'DESG_MI'
      and pibca1.cod_pais = 'ESPAÑA'
      and pibca1.resolucion = 'PT15M'
      and pibca1.fec_generacion > :market_datetime - :market_history_day * interval '1' day
      and pibca1.fec_generacion <= :market_datetime
    group by pibca1.fec_mercado,
             pibca1.id_hora,
             pibca1.cod_entidad
), pibca2 as (
   select pibca2.fec_mercado as market_dates,
          pibca2.id_hora as market_periods,
          pibca2.cod_entidad as ufi,
          abs(any_value(pibca2.valor_magnitud) keep(dense_rank first order by pibca2.version desc nulls last)) / 4 as position_megawatt_hour
     from XXXX_XXXX pibca2
    where pibca2.cod_mercado = 'D_MI'
      and pibca2.num_sesion = '2'
      and pibca2.cod_entidad in (:dim_ufi_bess_grid_import, :dim_ufi_bess_grid_export, :dim_ufi_res_grid_export)
      and pibca2.tipo_entidad = 'UFI'
      and pibca2.fec_mercado >= trunc(:market_datetime, 'DD')
      and pibca2.fec_mercado < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pibca2.tipo_oferta = 'DESG_MI'
      and pibca2.cod_pais = 'ESPAÑA'
      and pibca2.resolucion = 'PT15M'
      and pibca2.fec_generacion > :market_datetime - :market_history_day * interval '1' day
      and pibca2.fec_generacion <= :market_datetime
    group by pibca2.fec_mercado,
             pibca2.id_hora,
             pibca2.cod_entidad
), pibca3 as (
   select pibca3.fec_mercado as market_dates,
          pibca3.id_hora as market_periods,
          pibca3.cod_entidad as ufi,
          abs(any_value(pibca3.valor_magnitud) keep(dense_rank first order by pibca3.version desc nulls last)) / 4 as position_megawatt_hour
     from XXXX_XXXX pibca3
    where pibca3.cod_mercado = 'D_MI'
      and pibca3.num_sesion = '3'
      and pibca3.cod_entidad in (:dim_ufi_bess_grid_import, :dim_ufi_bess_grid_export, :dim_ufi_res_grid_export)
      and pibca3.tipo_entidad = 'UFI'
      and pibca3.fec_mercado >= trunc(:market_datetime, 'DD')
      and pibca3.fec_mercado < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and pibca3.tipo_oferta = 'DESG_MI'
      and pibca3.cod_pais = 'ESPAÑA'
      and pibca3.resolucion = 'PT15M'
      and pibca3.fec_generacion > :market_datetime - :market_history_day * interval '1' day
      and pibca3.fec_generacion <= :market_datetime
    group by pibca3.fec_mercado,
             pibca3.id_hora,
             pibca3.cod_entidad
), positions as (
   -- Cannot take matched price for the positions using marketx, so it does
   -- not exist here. It should not be a problem.
   select market.market_dates,
          market.market_periods,
          coalesce(pibca3.ufi, pibca2.ufi, pibca1.ufi, pdbf.ufi) as ufi,
          any_value(coalesce(pibca3.position_megawatt_hour, pibca2.position_megawatt_hour, pibca1.position_megawatt_hour, pdbf.position_megawatt_hour)) as position_megawatt_hour
     from market
     left join pdbf
       on market.market_dates = pdbf.market_dates
      and market.market_periods = pdbf.market_periods
      and :market_type in ('MI1', 'MI2', 'MI3', 'MIC')
     left join pibca1
       on market.market_dates = pibca1.market_dates
      and market.market_periods = pibca1.market_periods
      and :market_type in ('MI2', 'MI3', 'MIC')
     left join pibca2
       on market.market_dates = pibca2.market_dates
      and market.market_periods = pibca2.market_periods
      and :market_type in ('MI3', 'MIC')
     left join pibca3
       on market.market_dates = pibca3.market_dates
      and market.market_periods = pibca3.market_periods
      and :market_type in ('MIC')
    group by market.market_dates,
             market.market_periods,
             coalesce(pibca3.ufi, pibca2.ufi, pibca1.ufi, pdbf.ufi)
), limits as (
   -- XXXX_XXXX is mainly interested in the limits.
   select limits.fec_limitacionsuj as market_dates,
          4 * (extract(hour from cast(limits.hora_ini as timestamp)) + 1) + ceil(extract(minute from cast(limits.hora_ini as timestamp)) / 15) + 1 as market_periods,
          limits.cod_ufi as ufi,
          any_value(limits.pot_limite) keep(dense_rank first order by limits.version desc nulls last) as limit_megawatt
     from XXXX_XXXX limits
    where limits.cod_up = :dim_up_grid_export
      and (limits.cod_ufi in (:dim_ufi_bess_grid_export, :dim_ufi_res_grid_export) or limits.cod_ufi is null)
      and limits.cod_pais = 'ES'
      and limits.fec_version > :market_datetime - :market_history_day * interval '1' day
      and limits.fec_version <= :market_datetime
      and limits.fec_limitacionsuj >= trunc(:market_datetime, 'DD')
      and limits.fec_limitacionsuj < trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day
      and limits.hora_ini_utc >= cast(from_tz(cast(trunc(:market_datetime, 'DD') as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and limits.hora_ini_utc < cast(from_tz(cast(trunc(:market_datetime, 'DD') + :market_horizon_day * interval '1' day as timestamp), 'Europe/Madrid') at time zone 'UTC' as date)
      and limits.resolucion = 'PT15M'
    group by limits.fec_limitacionsuj,
             4 * (extract(hour from cast(limits.hora_ini as timestamp)) + 1) + ceil(extract(minute from cast(limits.hora_ini as timestamp)) / 15) + 1,
             limits.cod_ufi
)
select market.market_dates,
       market.market_periods,
       prices.market_types,
       prices.market_sessions,
       case when market.market_dates + (market.market_periods - 1) * interval '15' minute >= :market_datetime then coalesce(prices.price_euro_per_megawatt_hour, 0.0) else null end as market_price_euro_per_megawatt_hour,
       coalesce(energies.energy_megawatt_hour, 0.0) as res_export_megawatt_hour,
       coalesce(bess_grid_import_positions.position_megawatt_hour, 0.0) as bess_grid_import_matched_megawatt_hour,
       coalesce(bess_grid_export_positions.position_megawatt_hour, 0.0) as bess_grid_export_matched_megawatt_hour,
       coalesce(res_grid_export_positions.position_megawatt_hour, 0.0) as res_grid_export_matched_megawatt_hour,
       coalesce(bess_limits.limit_megawatt, binary_double_infinity) as bess_grid_export_limits_megawatt,
       coalesce(res_limits.limit_megawatt, binary_double_infinity) as res_grid_export_limits_megawatt,
       coalesce(grid_limits.limit_megawatt, binary_double_infinity) as grid_export_limits_megawatt
  from market
  left join prices
    on market.market_dates = prices.market_dates
   and market.market_periods = prices.market_periods
  left join energies
    on market.market_dates = energies.market_dates
   and market.market_periods = energies.market_periods
  left join positions bess_grid_import_positions
    on market.market_dates = bess_grid_import_positions.market_dates
   and market.market_periods = bess_grid_import_positions.market_periods
   and bess_grid_import_positions.ufi = :dim_ufi_bess_grid_import
  left join positions bess_grid_export_positions
    on market.market_dates = bess_grid_export_positions.market_dates
   and market.market_periods = bess_grid_export_positions.market_periods
   and bess_grid_export_positions.ufi = :dim_ufi_bess_grid_export
  left join positions res_grid_export_positions
    on market.market_dates = res_grid_export_positions.market_dates
   and market.market_periods = res_grid_export_positions.market_periods
   and res_grid_export_positions.ufi = :dim_ufi_res_grid_export
  left join limits bess_limits
    on market.market_dates = bess_limits.market_dates
   and market.market_periods = bess_limits.market_periods
   and bess_limits.ufi = :dim_ufi_bess_grid_export
  left join limits res_limits
    on market.market_dates = res_limits.market_dates
   and market.market_periods = res_limits.market_periods
   and res_limits.ufi = :dim_ufi_res_grid_export
  left join limits grid_limits
    on market.market_dates = grid_limits.market_dates
   and market.market_periods = grid_limits.market_periods
   and grid_limits.ufi is null
 order by market.market_dates asc,
          market.market_periods asc
