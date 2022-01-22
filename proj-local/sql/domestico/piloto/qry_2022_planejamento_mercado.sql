

SELECT * except(itens_mercado)
, ARRAY(SELECT AS STRUCT item, codigo, descricao, vl_unitario, tipo_unid, qtd, vl_item
FROM UNNEST(itens_mercado) itens
WHERE itens.tipo_unid = 'UNID'
order by item asc) item_unidade
, ARRAY(SELECT AS STRUCT item, codigo, descricao, vl_unitario, tipo_unid, qtd, vl_item
FROM UNNEST(itens_mercado) itens
WHERE itens.tipo_unid = 'KG'
order by item asc) item_kilo
FROM (


select valor_total, data_compra, mercado, forma_pagamento, mes_base, ano_base, mes_base_ordem
, array_agg(struct(item, codigo, descricao, vl_unitario, tipo_unid, qtd, vl_item)) itens_mercado

from devsamelo2.dev_domestico.lista_compras_mensal
group by valor_total, data_compra, mercado, forma_pagamento, mes_base, ano_base, mes_base_ordem
)