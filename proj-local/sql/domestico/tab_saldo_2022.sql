BEGIN

DECLARE JAN_2021 DATE DEFAULT '2021-01-01';
DECLARE FEV_2021 DATE DEFAULT '2021-02-01';
DECLARE MAR_2021 DATE DEFAULT '2021-03-01';
DECLARE ABR_2021 DATE DEFAULT '2021-04-01';
DECLARE MAI_2021 DATE DEFAULT '2021-05-01';
DECLARE JUN_2021 DATE DEFAULT '2021-06-01';
DECLARE JUL_2021 DATE DEFAULT '2021-07-01';
DECLARE AGO_2021 DATE DEFAULT '2021-08-01';
DECLARE SET_2021 DATE DEFAULT '2021-09-01';
DECLARE OUT_2021 DATE DEFAULT '2021-10-01';
DECLARE NOV_2021 DATE DEFAULT '2021-11-01';
DECLARE DEZ_2021 DATE DEFAULT '2021-12-01';

DECLARE JAN_2022 DATE DEFAULT '2022-01-01';
DECLARE FEV_2022 DATE DEFAULT '2022-02-01';
DECLARE MAR_2022 DATE DEFAULT '2022-03-01';
DECLARE ABR_2022 DATE DEFAULT '2022-04-01';


create  temp table receb as

    select dt_mes_base, 
            sum(valor_recebido) valor_recebido

    from (
    select descricao, 
            dt_recebido , 
            dt_mes_base,
            valor_recebido,
            row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.recebido_2021_excel`
   union all 

    select descricao, 
            dt_recebido , 
            dt_mes_base,
            valor_recebido,
            row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.recebido_2022_excel`
   union all 
    select descricao
        , dt_recebido 
        , dt_mes_base
        , valor_recebido
        , 1 ordem_recebimento
from     `devsamelo2.dev_domestico.recebido_forms`
    ) 
    where ordem = 1
    group by dt_mes_base;


--> base do extrato do banco itau, extração da saida(gastos)

create  temp table  custo_base as

    select dt_mes_base, 
            sum(valor_custo)  valor_custo ,
      from (
        select tipo_custo,
                custo,	
                valor_custo,	
                dt_mes_base,	
                dt_custo,
                row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
            from `devsamelo2.dev_domestico.custo_2021_excel`
union all
        select tipo_custo,
                custo,	
                valor_custo,	
                dt_mes_base,	
                dt_custo,
                row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
            from `devsamelo2.dev_domestico.custo_2022_excel`  
   union all 
SELECT tipo_custo
       , custo

       , valor_custo
       , data_base_bq dt_mes_base
       , dt_custo_bq dt_custo
      , 1 ordem       
FROM `devsamelo2.dev_domestico.custo_forms`
where pendente = 'Sim'
  and ano_base = 2022                     
    ) 
    where ordem = 1
    group by dt_mes_base;




--> base do extrato do banco itau, extração do saldo que exige uma regra(primeiro saldo do mes)
--> entra como custo do mes seguinte
create  temp table  saldo as
select dt_mes_base, 
        sum(saldo) saldo
    from (
        select descricao,
               dt_recebido, 
               dt_mes_base, 
               saldo,
               row_number() over (partition by  descricao,	dt_recebido	, cast(saldo as string),	dt_mes_base order by process_time desc) ordem,
               row_number() over (partition by 	dt_mes_base order by dt_recebido, process_time asc) ordem_prim_sald
         from `devsamelo2.dev_domestico.saldo_2021_excel`
         WHERE saldo  is not null
union all 
         select descricao,
               dt_recebido, 
               dt_mes_base, 
               saldo,
               row_number() over (partition by  descricao,	dt_recebido	, cast(saldo as string),	dt_mes_base order by process_time desc) ordem,
               row_number() over (partition by 	dt_mes_base order by dt_recebido, process_time asc) ordem_prim_sald
         from   `devsamelo2.dev_domestico.saldo_2022_excel` 
         WHERE saldo  is not null        
    )
    where ordem = 1
      and ordem_prim_sald = 1
    group by dt_mes_base
;


create or replace table devsamelo2.dev_domestico.saldo_mes as 
--> base do excel inserção manual de valores de saida, num base resultado da consolidação dos dados
--> usado para meses futuros - somente gastos futuros
-- with custo_fake as (
--     select dt_mes_base, 
--             sum(valor_custo)  valor_custo ,
--       from `devsamelo2.dev_domestico.custo_consolidado`
--     group by dt_mes_base
-- )

--> base do extrato do banco itau, extração da entrada(salarios e outros)


--> consolida mensal - mes a mes
with  jan_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + if(saldo is null, 0, saldo)-2 saldo, 
        FEV_2021 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JAN_2021 )
     where  rec.dt_mes_base = JAN_2021
    ) rec
left join saldo sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = JAN_2021 )
where  rec.dt_mes_base = JAN_2021
order by rec.dt_mes_base
)

, fev_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        MAR_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = FEV_2021 )
     where  rec.dt_mes_base = FEV_2021
    ) rec
left join jan_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = FEV_2021 )
where  rec.dt_mes_base = FEV_2021
order by rec.dt_mes_base
)

, mar_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo,  
        ABR_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAR_2021 )
     where  rec.dt_mes_base = MAR_2021
    ) rec
left join fev_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAR_2021 )
where  rec.dt_mes_base = MAR_2021
order by rec.dt_mes_base
)


, abr_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        MAI_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = ABR_2021 )
     where  rec.dt_mes_base = ABR_2021
    ) rec
left join mar_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = ABR_2021 )
where  rec.dt_mes_base = ABR_2021
order by rec.dt_mes_base
)

, mai_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        JUN_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAI_2021 )
     where  rec.dt_mes_base = MAI_2021
    ) rec
left join abr_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = MAI_2021 )
where  rec.dt_mes_base = MAI_2021
order by rec.dt_mes_base
)

, jun_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        JUL_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JUN_2021 )
     where  rec.dt_mes_base = JUN_2021
    ) rec
left join mai_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = JUN_2021 )
where  rec.dt_mes_base = JUN_2021
order by rec.dt_mes_base
)


, jul_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        AGO_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JUL_2021 )
     where  rec.dt_mes_base = JUL_2021
    ) rec
left join jun_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = JUL_2021 )
where  rec.dt_mes_base = JUL_2021
order by rec.dt_mes_base
)

, ago_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        SET_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = AGO_2021 )
     where  rec.dt_mes_base = AGO_2021
    ) rec
left join jul_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = AGO_2021 )
where  rec.dt_mes_base = AGO_2021
order by rec.dt_mes_base
)

, set_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        OUT_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = SET_2021 )
     where  rec.dt_mes_base = SET_2021
    ) rec
left join ago_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = SET_2021 )
where  rec.dt_mes_base = SET_2021
order by rec.dt_mes_base
)

, out_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        NOV_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = OUT_2021 )
     where  rec.dt_mes_base = OUT_2021
    ) rec
left join set_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = OUT_2021 )
where  rec.dt_mes_base = OUT_2021
order by rec.dt_mes_base
)


, nov_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo, 
        DEZ_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = NOV_2021 )
     where  rec.dt_mes_base = NOV_2021
    ) rec
left join out_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = NOV_2021 )
where  rec.dt_mes_base = NOV_2021
order by rec.dt_mes_base
)

, dez_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + sald.saldo saldo,  
        JAN_2022 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = DEZ_2021 )
     where  rec.dt_mes_base = DEZ_2021
    ) rec
left join nov_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = DEZ_2021 )
where  rec.dt_mes_base = DEZ_2021
order by rec.dt_mes_base
)

, jan_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + if(saldo is null, 0, saldo) saldo, 
        FEV_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JAN_2022 )
     where  rec.dt_mes_base = JAN_2022
    ) rec
left join saldo sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = JAN_2022 )
where  rec.dt_mes_base = JAN_2022
order by rec.dt_mes_base
)

, fev_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + if(saldo is null, 0, saldo) saldo, 
        MAR_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = FEV_2022 )
     where  rec.dt_mes_base = FEV_2022
    ) rec
left join jan_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = FEV_2022 )
where  rec.dt_mes_base = FEV_2022
order by rec.dt_mes_base
)

, mar_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_custo, 
        saldo saldo_inicial, 
        rec.valor_recebido-rec.valor_custo + if(saldo is null, 0, saldo) saldo, 
        FEV_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.valor_custo
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAR_2022 )
     where  rec.dt_mes_base = MAR_2022
    ) rec
left join fev_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = MAR_2022 )
where  rec.dt_mes_base = MAR_2022
order by rec.dt_mes_base
)

select * from (

select dt_mes_base,	valor_recebido,	valor_custo, saldo from jan_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from fev_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from mar_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from abr_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from mai_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from jun_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from jul_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from ago_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from set_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from out_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from nov_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from dez_2021
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from jan_2022
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from fev_2022
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from mar_2022
)
order by dt_mes_base

;END;