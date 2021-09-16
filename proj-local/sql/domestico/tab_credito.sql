create or replace table devsamelo2.dev_domestico.credito_mes as 
    select tipo_custo_credito
    , custo_credito
    , dt_mes_base
    , dt_credito
    ,     extract(year from dt_mes_base) ano_base
    , extract(month from dt_mes_base) mes_base_ordem
        , case when extract(month from dt_mes_base) = 1 then 'JAN' 
            when extract(month from dt_mes_base) = 2 then 'FEV' 
            when extract(month from dt_mes_base) = 3 then 'MAR' 
            when extract(month from dt_mes_base) = 4 then 'ABR' 
            when extract(month from dt_mes_base) = 5 then 'MAI' 
            when extract(month from dt_mes_base) = 6 then 'JUN' 
            when extract(month from dt_mes_base) = 7 then 'JUL' 
            when extract(month from dt_mes_base) = 8 then 'AGO' 
            when extract(month from dt_mes_base) = 9 then 'SET' 
            when extract(month from dt_mes_base) = 10 then 'OUT' 
            when extract(month from dt_mes_base) = 11 then 'NOV' 
            when extract(month from dt_mes_base) = 12 then 'DEZ' 
            else null end mes_base
    , sum(valor_credito) valor_credito

    from (
    select tipo_custo_credito,custo_credito, dt_credito , dt_mes_base,valor_credito,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2021_excel`
   -- where dt_mes_base = '2021-10-01'
    ) where ordem = 1

   group by tipo_custo_credito, custo_credito, dt_credito, dt_mes_base
    order by  dt_mes_base--,dt_credito
