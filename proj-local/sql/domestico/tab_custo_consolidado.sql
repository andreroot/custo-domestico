create or replace table devsamelo2.dev_domestico.custo_consolidado as (
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
        , row_number() over (partition by tipo_custo,	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`) 
    where ordem = 1)

,hist as(
select *,
extract(year from data_base_bq) ano_base,
extract(month from data_base_bq) mes_base_ordem,
case when extract(month from data_base_bq) = 1 then 'JAN' 
    when extract(month from data_base_bq) = 2 then 'FEV' 
    when extract(month from data_base_bq) = 3 then 'MAR' 
    when extract(month from data_base_bq) = 4 then 'ABR' 
    when extract(month from data_base_bq) = 5 then 'MAI' 
    when extract(month from data_base_bq) = 6 then 'JUN' 
    when extract(month from data_base_bq) = 7 then 'JUL' 
    when extract(month from data_base_bq) = 8 then 'AGO' 
    when extract(month from data_base_bq) = 9 then 'SET' 
    when extract(month from data_base_bq) = 10 then 'OUT' 
    when extract(month from data_base_bq) = 11 then 'NOV' 
    when extract(month from data_base_bq) = 12 then 'DEZ' 
    else null end mes_base
from (
select 
dt_pagto_bq data_base_bq,

custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo2.dev_domestico.custo_2015
union all 
select 
dt_pagto_bq data_base_bq,

custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo2.dev_domestico.custo_2016
union all 
select 
dt_pagto_bq data_base_bq,

custo,
tipo_custo,
dt_custo_bq,
valor_custo,
from devsamelo2.dev_domestico.custo_2017
union all 
select 
dt_pagto_bq data_base_bq,

custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo2.dev_domestico.custo_2018
union all 
select 
dt_pagto_bq data_base_bq,

custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo2.dev_domestico.custo_2019
union all 
select 
dt_pagto_bq data_base_bq,

custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo2.dev_domestico.custo_2020
union all 
select 
dt_mes_base data_base_bq,

custo,
tipo_custo,
dt_custo dt_custo_bq,
valor_custo,
from custo_2021
)

)

select * , 'consolidado' as source from hist
union all
SELECT data_base_bq
       , custo
       , tipo_custo
       , dt_custo_bq
       , valor_custo
       , ano_base
       , mes_base_ordem
       , mes_base
       , 'previsão' as source 
FROM `devsamelo2.dev_domestico.custo_forms_2021`
where pendente = 'Sim'
)