--create or replace table devsamelo2.dev_domestico.custo_vida_deb_versus_cred as 

with custo_2021 as 
    (select --* except(ordem)  
     tipo_custo
        ,	sum(valor_custo) valor_custo
        ,	dt_mes_base
        ,	dt_custo
      from (
    select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo
        , row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`) 
    where ordem = 1
    group by tipo_custo
        ,	dt_mes_base
        ,	dt_custo)



select 
            tipo_custo
        ,	dt_mes_base
        ,	dt_custo
        ,extract(week from dt_mes_base) semana_base        
        ,extract(year from dt_mes_base) ano_base
        ,extract(month from dt_mes_base) mes_base_ordem
        ,case when extract(month from dt_mes_base) = 1 then 'JAN' 
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
    ,	valor_custo
    , valor_credito_parc
    , if(valor_credito_parc is null, 0, valor_credito_parc+valor_custo) total_geral
from  custo_2021 deb
FULL OUTER JOIN (
    select tipo_custo_credito,
            dt_credito ,
            sum(valor_credito_parc) valor_credito_parc
    from (
    select tipo_custo_credito,
            dt_credito , 
            valor_credito_parc,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito_parc as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2021_excel`
   -- where dt_mes_base = '2021-10-01'
    ) where ordem = 1
    group by tipo_custo_credito,
            dt_credito 

) cred on (cred.tipo_custo_credito = deb.tipo_custo and cred.dt_credito = deb.dt_custo )  
where deb.dt_mes_base >= '2021-01-01' --,'2021-09-01','2021-07-01','2021-06-01')
and deb.tipo_custo in ('mercado','alimentação','compras','farmacia')

order by dt_mes_base
