create or replace table devsamelo2.dev_domestico.custo_comparativo as (
with custo_2021 as 
    (select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo   
      from (
    select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo
        , row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`) 
    where ordem = 1)

, ano_atual as(

    select 
        dt_mes_base data_base_bq,
        extract(month from dt_mes_base) mes_base_ordem,
        tipo_custo descricao,
        --lan__amento lancamento,
        sum(valor_custo) valor_2021,
    from custo_2021 atl
    group by dt_mes_base, mes_base_ordem, tipo_custo--, lan__amento

    
)


select atl.*
,case when extract(month from atl.data_base_bq) = 1 then 'JAN' 
    when extract(month from atl.data_base_bq) = 2 then 'FEV' 
    when extract(month from atl.data_base_bq) = 3 then 'MAR' 
    when extract(month from atl.data_base_bq) = 4 then 'ABR' 
    when extract(month from atl.data_base_bq) = 5 then 'MAI' 
    when extract(month from atl.data_base_bq) = 6 then 'JUN' 
    when extract(month from atl.data_base_bq) = 7 then 'JUL' 
    when extract(month from atl.data_base_bq) = 8 then 'AGO' 
    when extract(month from atl.data_base_bq) = 9 then 'SET' 
    when extract(month from atl.data_base_bq) = 10 then 'OUT' 
    when extract(month from atl.data_base_bq) = 11 then 'NOV' 
    when extract(month from atl.data_base_bq) = 12 then 'DEZ' 
    else null end mes_base
     , old_20.valor_2020
     , old_19.valor_2019
  from ano_atual atl
left join (
    select 
        dt_pagto_bq data_base_bq,
        extract(month from dt_pagto_bq) mes_base_ordem,
        tipo_custo descricao_2020,
        --lan__amento lancamento,
        sum(valor_custo) valor_2020,
    from devsamelo2.dev_domestico.custo_2020 old
    group by dt_pagto_bq, mes_base_ordem, tipo_custo--, lan__amento
) old_20 
on (atl.mes_base_ordem = old_20.mes_base_ordem 
and atl.descricao = old_20.descricao_2020 )
left join (
    select
            dt_pagto_bq data_base_bq,
            extract(month from dt_pagto_bq) mes_base_ordem,
            tipo_custo descricao_2019,
            sum(valor_custo) valor_2019,
    from devsamelo2.dev_domestico.custo_2019 old
   group by dt_pagto_bq, mes_base_ordem, tipo_custo--, lan__amento
) old_19 
on (atl.mes_base_ordem = old_19.mes_base_ordem 
and atl.descricao = old_19.descricao_2019 )
)
