from bq.insert_bq import InsertBq
from bucket.mov_file_bucket import MovimentacaoFileBucket
from pandadf.generate_df import GenerateDF

# --> 2021"descrição";"data";"lançamento";"ag./origem";"valor (R$)";"valor_convertido_saida (R$)";"valor_convertido_entrada (R$)";"REF"
# --> 2020"descrição";"data";"lançamento";"ag./origem";"valor (R$)";"valor_convertido_saida (R$)";"valor_convertido_entrada (R$)";"REF"
# --> 2019"custo";"valor_custo";"flag_fixo";"flag_variavel";"flag_pendente";"flag_investimento";"data_base";"data_custo";"tipo_custo";
# --> 2018"custo";"valor_custo";"flag_fixo";"flag_variavel";"flag_pendente";"data_base";"semana";"data_custo";"tipo_custo";
# --> 2017"custo";"valor debito";"custo fixo";"custo variavel";"pendente";"data pagto";"semana";"data pagamento";"tipo"
# --> 2016"custo";"valor debito";"custo fixo";"custo variavel";"pendente";"data pagto";"semana";"data pagamento";"tipo";;
# --> 2015"custo";"valor debito";"custo fixo";"custo variavel";"pendente";"data pagto";"semana";"data pagamento";"tipo";

#--> CONVERTER CAMPOS DOS CSV PARA FORMATO BQ:
#--> VALOR - ALTERAR DE "," PARA "." - NAS CONFIGURAÇÕES DO TIPO DE DADOS PARA FORMATO USA
#--> DATA - ALTERAR FORMATO PARA "YYYY-MM-DD" - APLICAR =DATE(RIGHT(H2;4);MID(H2;4;2);LEFT(H2;2)) E NAS CONFIGURAÇÕES NO FORMATO USA
#--> NOVOS CAMPÓS DE DATA COM FORMATO BQ: dt_pagto_bq	dt_custo_bq
#--> REMOVER GASTOS COM FOLHA DE PAGAMENTO - ANOS 2015 / 2016 / 2017 |  E META: 2019 / 2018
#--> PADRONIZAR NOMES DOS TIPO DE CUSTOS:   custos estão agrupados no campo tipo_custo(2015 / 2016 / 2017 / 2018 / 2019)
#-->                                        custos estão agrupados no campo descri____o(2020 / 2021)  

def df_csv_depara_debito(file_in):

    #--> return df
    df = GenerateDF().csv_generate_df_depara_debito('csv/domestico/'+file_in)

    return df

def df_csv_custo(file_in):

    #--> return df
    df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
    #df = GenerateDF().pandas_teste()

    return df

def execute_qery(queri):

    #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
    MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'sql/domestico/', 'sql/', queri)

    #--> pegar arquivo no storage - parametros: nome do bucket, diretorio do bucket, arquivo
    queri_comparativo = MovimentacaoFileBucket().pull_file('proj-domestico-file', 'sql/', queri)

    #--> executar query
    ret_comparativo = InsertBq.create_job(queri_comparativo)

def convert(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base+".csv"
    #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
    MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'csv/domestico/', 'extrator/', file_in)
    #--> nome da tabela que sera gerada no biquery
    dataset = 'dev_domestico'
    tabela_bq = base
    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_tabela( dataset, tabela_bq, 'proj-domestico-file/', 'extrator/', file_in)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_comparativo = "custo_comparativo.sql"
    execute_qery(q_comparativo)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_consolidado = "custo_consolidado.sql"
    execute_qery(q_consolidado)

def convert_depara_info(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base+".csv"

    gen_df = df_csv_custo(file_in)

    dataset = 'dev_domestico'
    tabela_bq = base
    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_df_depara(gen_df, dataset, base)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    #q_comparativo = "custo_comparativo.sql"
    #execute_qery(q_comparativo)

def convert_custo(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base+".csv"

    gen_df = df_csv_custo(file_in)

    dataset = 'dev_domestico'
    tabela_bq = base
    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_df_custo(gen_df, dataset, base)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    #q_comparativo = "custo_comparativo.sql"
    #execute_qery(q_comparativo)

convert_custo('custo_2020')