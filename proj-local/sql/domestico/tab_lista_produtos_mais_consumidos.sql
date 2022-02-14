/*frequencia de compra*/
create or replace table devsamelo2.dev_domestico.lista_produtos_mais_consumidos as 
with
primeira_compra_all as (


select codigo
, descricao
, dt_mes_base
,  date(data_compra)  data_compra   
, vl_unitario              
, row_number() over ( partition by codigo  order by data_compra asc) total_compras
   from devsamelo2.dev_domestico.lista_compras
   WHERE tipo_unid = 'UNID'
     and mercado = 'TRIMAIS'
     --and codigo = 7891025101604
     --REGEXP_EXTRACT(query, r"[^\\b]CREATE[^\\b]TABLE[^\\b]IF[^\\b]NOT[^\\b]EXISTS[^\\b]+[a-zA-Z0-9_.+-]+[^\\b]")
     order by data_compra  desc


)


select codigo
, descricao
, dt_mes_base
, vl_unitario
, prim_data_compra_item
, data_compra as segunda_data_compra_item
, date_diff(data_compra, prim_data_compra_item, day) periodo_primeira_compra_item

--, cast(100/cast((total_compra_geral / date_diff(ultim_data_compra_geral, prim_data_compra_geral, day))*100 as int) as int) freq_dias_mercado

--, cast((total_compras / total_compra_geral)*100 as int) freq_dias_item_mercado


--, date_diff(ultim_data_compra_geral, prim_data_compra_geral, day) total_dias
, (select date( data_compra) 
    from primeira_compra_all a
    where a.codigo = one.codigo and a.total_compras = total_vezes_compra_item-1) as penult_compra_item

, total_vezes_compra_item
, last_date_compra_item


--, total_compras freq_compra_item
, cast((total_vezes_compra_item / total_ida_mercado)*100 as int) probabilidade_compra_item --qual a chane de compra do item na proxima ida
--, prim_data_compra_geral


, date_diff(last_date_compra_item, (select date( data_compra) 
    from primeira_compra_all a
    where a.codigo = one.codigo and a.total_compras = total_vezes_compra_item-1), day) periodo_ultima_compra

, date_add(last_date_compra_item, interval date_diff(last_date_compra_item, (select date( data_compra) 
    from primeira_compra_all a
    where a.codigo = one.codigo and a.total_compras = total_vezes_compra_item-1), day) day) prev_periodo_prox_compra

, total_ida_mercado total_vezes_mercado
, last_date_compra_geral

from 

(
select * 
/**/
, (select max(total_compras)
    from primeira_compra_all a where a.codigo = one.codigo) as total_vezes_compra_item

, (select max(date( data_compra) ) 
    from primeira_compra_all a where a.codigo = one.codigo) as last_date_compra_item

, (select min(date( data_compra) ) 
    from primeira_compra_all a
    where a.codigo = one.codigo and a.total_compras = 1) as prim_data_compra_item

, (select count(distinct data_compra)
   from devsamelo2.dev_domestico.lista_compras
   WHERE tipo_unid = 'UNID'
     and mercado = 'TRIMAIS') total_ida_mercado

, (select max( data_compra)
   from devsamelo2.dev_domestico.lista_compras
   WHERE tipo_unid = 'UNID'
     and mercado = 'TRIMAIS') last_date_compra_geral

/**/
from primeira_compra_all one
where one.total_compras = 2


) one
order by probabilidade_compra_item desc


