CREATE TEMP FUNCTION saldo(x FLOAT64, y FLOAT64)
RETURNS FLOAT64
LANGUAGE js AS r"""
  return x*y;
""";

CREATE TEMP TABLE users
AS SELECT 1 id, 10 age
UNION ALL SELECT 2, 30
UNION ALL SELECT 3, 10;

CREATE TEMP FUNCTION countUserByAge(userAge INT64)
AS ((SELECT COUNT(1) FROM users WHERE age = userAge));

with saldo_inicial_jan as (
SELECT
  dt_mes_base,
  source,
  if(source = 'Previsto', sum(valor_recebido_previsao),SUM(valor_recebido)) valor_recebido,
  valor_custo,
  if(source = 'Previsto', sum(valor_recebido_previsao),SUM(valor_recebido)) - valor_custo AS saldo,
  rank() over (order by dt_mes_base) as ordem, --partition by dt_mes_base
  DATE_TRUNC(date_add(dt_mes_base, interval 1 month), month) data_mes_posterior
FROM
  `devsamelo2.dev_domestico.recebido_previsto` ent
LEFT JOIN (
  SELECT
    data_base_bq,
    SUM(valor_custo) valor_custo
  FROM
    `devsamelo2.dev_domestico.custo_consolidado`
    --where data_base_bq = DATE_TRUNC(date_sub(current_date(), interval 1 month), month)
  GROUP BY
    data_base_bq ) cst
ON
  (ent.dt_mes_base = cst.data_base_bq)
 --where dt_mes_base = '2021-03-01'
  --where dt_mes_base = DATE_TRUNC(date_sub(current_date(), interval 1 month), month)
GROUP BY
  dt_mes_base,
  source,
  valor_custo
ORDER BY
  1 ASC)


  select atual.*, atual.saldo+ini.saldo saldo_real, multiplyInputs(x, y) as product
   from saldo_inicial atual
  left join saldo_inicial ini on (atual.dt_mes_base = ini.data_mes_posterior)
ORDER BY
  dt_mes_base







select rec.dt_mes_base,valor_recebido, valor_custo, saldo, valor_recebido-valor_custo+if(saldo is null, 0, saldo) from 
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
row_number() over (partition by tipo_custo,	custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
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

order by rec.dt_mes_base
