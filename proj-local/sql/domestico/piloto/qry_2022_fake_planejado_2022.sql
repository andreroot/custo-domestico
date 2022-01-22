with custo_base as (

    select dt_mes_base,	
            tipo_custo,
            sum(valor_custo) as custo_total,	

      from (
        select tipo_custo,
                custo,	
                valor_custo,	
                dt_mes_base,	
                dt_custo,
                row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
            from `devsamelo2.dev_domestico.custo_2021_excel`
    ) 
    where ordem = 1
    group by dt_mes_base,	
            tipo_custo

)

select pl.dt_mes_base,
        pl.valor_recebido, 
        pl.valor_custo, 
        pl.saldo
        , array_agg(struct(cst. dt_mes_base,	
           cst.  tipo_custo,
           cst.  custo_total)) custo
  from devsamelo2.dev_domestico.saldo_mes pl inner join custo_base cst on (pl.dt_mes_base = cst.dt_mes_base)
  group by pl.dt_mes_base,
        pl.valor_recebido, 
        pl.valor_custo, 
        pl.saldo
