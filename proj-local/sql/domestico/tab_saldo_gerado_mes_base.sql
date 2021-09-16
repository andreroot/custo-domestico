create or replace table devsamelo2.dev_domestico.saldo_mes as 
with jan as (
select rec.dt_mes_base,valor_recebido, valor_custo, saldo saldo_inicial, valor_recebido-valor_custo+if(saldo is null, 0, saldo) saldo, '2021-02-01' dt_mes_base_saldo from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join (

select dt_mes_base, sum(saldo) saldo

    from (
        select descricao, dt_recebido , dt_mes_base, saldo
        ,row_number() over (partition by descricao,	dt_recebido	, cast(saldo as string),	dt_mes_base order by process_time desc) ordem
        ,row_number() over (partition by 	dt_mes_base order by dt_recebido, process_time asc) ordem_prim_sald

        from `devsamelo2.dev_domestico.saldo_2021_excel`
        where  dt_mes_base = '2021-01-01'
        and saldo  is not null
    )
    where ordem = 1
    and ordem_prim_sald = 1
    group by dt_mes_base
) sald  on rec.dt_mes_base = sald.dt_mes_base

where rec.dt_mes_base = '2021-01-01'
order by rec.dt_mes_base
)

, fev as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-03-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
        select descricao, dt_recebido , dt_mes_base,valor_recebido,
        row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
        from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join jan sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-02-01'
order by rec.dt_mes_base
)


, mar as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-04-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join fev sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-03-01'
order by rec.dt_mes_base
)


, abr as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-05-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join mar sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-04-01'
order by rec.dt_mes_base
)


, mai as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-06-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join abr sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-05-01'
order by rec.dt_mes_base
)

, jun as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-07-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join mai sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-06-01'
order by rec.dt_mes_base
)


, jul as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-08-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join jun sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-07-01'
order by rec.dt_mes_base
)

, ago as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-09-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join jul sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-08-01'
order by rec.dt_mes_base
)

, seb as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-10-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1

    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join ago sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-09-01'
order by rec.dt_mes_base
)

, out as (
select rec.dt_mes_base,rec.valor_recebido, cst.valor_custo, rec.valor_recebido-cst.valor_custo+sald.saldo saldo, '2021-11-01' dt_mes_base_saldo
from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (
/**alterar query para clacular previsão do mes seguinte
    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by 	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    ) where ordem = 1
**/
    select  dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base, dt_custo_bq,
    --row_number() over (partition by 	custo,	cast(valor_custo as string),	data_base_bq, dt_custo_bq order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_consolidado`
    ) --where ordem = 1
    group by dt_mes_base

) cst on rec.dt_mes_base = cst.dt_mes_base

left join seb sald  on rec.dt_mes_base = date(sald.dt_mes_base_saldo)

where rec.dt_mes_base = '2021-10-01'
order by rec.dt_mes_base
)

select * from (

select dt_mes_base,	valor_recebido,	valor_custo, saldo from jan
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from fev
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from mar
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from abr
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from mai
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from jun
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from jul
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from ago
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from seb
union all 
select dt_mes_base,	valor_recebido,	valor_custo, saldo from out
)
order by dt_mes_base