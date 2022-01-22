from bq.insert_bq import InsertBq
from bucket.mov_file_bucket import MovimentacaoFileBucket

# --> 2021"descrição";"data";"lançamento";"ag./origem";"valor (R$)";"valor_convertido_saida (R$)";"valor_convertido_entrada (R$)";"REF"
# --> 2020"descrição";"data";"lançamento";"ag./origem";"valor (R$)";"valor_convertido_saida (R$)";"valor_convertido_entrada (R$)";"REF"
# --> 2019"custo";"valor_custo";"flag_fixo";"flag_variavel";"flag_pendente";"flag_investimento";"data_base";"data_custo";"tipo_custo";
# --> 2018"custo";"valor_custo";"flag_fixo";"flag_variavel";"flag_pendente";"data_base";"semana";"data_custo";"tipo_custo";
# --> 2017"custo";"valor debito";"custo fixo";"custo variavel";"pendente";"data pagto";"semana";"data pagamento";"tipo"
# --> 2016"custo";"valor debito";"custo fixo";"custo variavel";"pendente";"data pagto";"semana";"data pagamento";"tipo";;
# --> 2015"custo";"valor debito";"custo fixo";"custo variavel";"pendente";"data pagto";"semana";"data pagamento";"tipo";

#--> CONVERTER CAMPOS DOS CSV PARA FORMATO BQ:
#--> VALOR - ALTERAR DE "," PARA "." - NAS CONFIGURAÇÕES DO TIPO DE DADOS PARA FORMATO USA
#--> DATA - ALTERAR FORMATO PARA "YYYY-MM-DD" - APLICAR =DATE(RIGHT(F2;4);MID(F2;4;2);LEFT(F2;2)) E NAS CONFIGURAÇÕES NO FORMATO USA
#--> NOVOS CAMPÓS DE DATA COM FORMATO BQ: dt_pagto_bq	dt_custo_bq
#--> REMOVER GASTOS COM FOLHA DE PAGAMENTO - ANOS 2015 / 2016 / 2017 |  E META: 2019 / 2018
#--> PADRONIZAR NOMES DOS TIPO DE CUSTOS:   custos estão agrupados no campo tipo_custo(2015 / 2016 / 2017 / 2018 / 2019)
#-->                                        custos estão agrupados no campo descri____o(2020 / 2021)  

def convert():

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = "obra_ng_conect_lan.csv"
    #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
    MovimentacaoFileBucket().upload_blob('proj-ngconect-file', 'csv/ng_conect', 'extrator/', file_in)
    #--> nome da tabela que sera gerada no biquery
    dataset = 'dev_dados_coletados'
    tabela_bq = "obra_ng_conect_lan"
    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq.insert_tabela( dataset, tabela_bq, 'proj-ngconect-file/', 'extrator/', file_in)

convert()