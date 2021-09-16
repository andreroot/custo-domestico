--> encontrado problema no calculo do sadlo, deve ser corrigdo

select rec.dt_mes_base,valor_recebido, valor_custo, saldo saldo_inicial, valor_recebido-valor_custo+if(saldo is null, 0, saldo) saldo, '2021-02-01' dt_mes_base_saldo from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    where dt_mes_base = '2021-01-01'
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    where dt_mes_base = '2021-01-01'
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
        and descricao = 'SALDO INICIAL'
    )
    where ordem = 1
    and ordem_prim_sald = 1
    group by dt_mes_base
) sald  on rec.dt_mes_base = sald.dt_mes_base


order by rec.dt_mes_base



select rec.dt_mes_base,valor_recebido, valor_custo, saldo saldo_inicial, valor_recebido-valor_custo+if(saldo is null, 0, saldo) saldo, '2021-02-01' dt_mes_base_saldo from 
(
    select dt_mes_base, sum(valor_recebido) valor_recebido

    from (
    select descricao, dt_recebido , dt_mes_base,valor_recebido,
    row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
    from `devsamelo2.dev_domestico.recebido_2021_excel`
    where dt_mes_base = '2021-02-01'
    ) where ordem = 1

    group by dt_mes_base


) rec
left join (

    select dt_mes_base, sum(valor_custo)  valor_custo ,
    from (
    select tipo_custo,	custo,	valor_custo,	dt_mes_base,	dt_custo,
    row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`
    where dt_mes_base = '2021-02-01'
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
        where  dt_mes_base = '2021-02-01'
        and descricao = 'SALDO INICIAL'
    )
    where ordem = 1
    and ordem_prim_sald = 1
    group by dt_mes_base
) sald  on rec.dt_mes_base = sald.dt_mes_base


order by rec.dt_mes_base