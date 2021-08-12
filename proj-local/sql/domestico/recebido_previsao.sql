create or replace table devsamelo2.dev_domestico.recebido_previsto as (

    select hist.* except(previsao)
    , prev.descricao as fonte_previsao
    , prev.dt_recebido as dt_recebido_previsao
    , prev.valor_recebido as valor_recebido_previsao
    , if(prev.valor_recebido is null, 'Historico', 'Previsto') as source
    from (
        SELECT hist.*, array_agg(struct(prev.descricao, prev.dt_recebido , prev.valor_recebido)) previsao ---  as dt_recebido_previsao as valor_recebido_previsao

        FROM `devsamelo2.dev_domestico.recebido_2021` hist

        left join `devsamelo2.dev_domestico.recebido_forms_2021` prev on (hist.dt_mes_base = prev.dt_mes_base and hist.descricao = "previsão")
        where hist.dt_mes_base >= '2021-01-01'

        group by hist.descricao, hist.dt_mes_base, hist.dt_recebido, hist.valor_recebido, hist.valor_previsto
        --SELECT * FROM `devsamelo2.dev_domestico.recebido_forms_2021`
    ) hist, unnest(previsao) prev


)