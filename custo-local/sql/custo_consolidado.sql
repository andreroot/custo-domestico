create or replace table devsamelo.dev_custo.custo_consolidado as (
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
tipo,
dt_custo_bq,
valor_debito,

from devsamelo.dev_custo.custo_2015
union all 
select 
dt_pagto_bq data_base_bq,
custo,
tipo,
dt_custo_bq,
valor_debito,

from devsamelo.dev_custo.custo_2016
union all 
select 
dt_pagto_bq data_base_bq,
custo,
tipo,
dt_custo_bq,
valor_debito,

from devsamelo.dev_custo.custo_2017
union all 
select 
dt_pagto_bq data_base_bq,
custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo.dev_custo.custo_2018
union all 
select 
dt_pagto_bq data_base_bq,
custo,
tipo_custo,
dt_custo_bq,
valor_custo,

from devsamelo.dev_custo.custo_2019
union all 
select 
dt_pagto_bq data_base_bq,
lan__amento,
descri____o,

dt_custo_bq,
valor_convertido_saida__R__,

from devsamelo.dev_custo.custo_2020
union all 
select 
dt_pagto_bq data_base_bq,
lan__amento,
descri____o,
dt_custo_bq,
valor_convertido_saida__R__,

from devsamelo.dev_custo.custo_2021
)
)