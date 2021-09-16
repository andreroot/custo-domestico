from bq.insert_bq import InsertBq
from bucket.mov_file_bucket import MovimentacaoFileBucket
from pandadf.generate_df_excel import GenerateDF

def df_excel_custo(file_in):

    #--> return df
    df = GenerateDF().excel_generate_df_custo('csv/domestico/excel/'+file_in)
    #excel_generate_df_recebido

    #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
    return df


def df_excel_credito(file_in):

    #--> return df
    df = GenerateDF().excel_generate_df_credito('csv/domestico/excel/'+file_in)
    #excel_generate_df_recebido

    #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
    return df

def df_excel_recebido(file_in):

    #--> return df
    df = GenerateDF().excel_generate_df_recebido('csv/domestico/excel/'+file_in)
    #excel_generate_df_recebido

    #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
    return df

def df_excel_saldo(file_in):

    #--> return df
    df = GenerateDF().excel_generate_df_saldo('csv/domestico/excel/'+file_in)
    #excel_generate_df_recebido

    #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
    return df    

def movimenta_file(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base+".csv"
    #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
    MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'csv/domestico/', 'extrator/', file_in)

def convert_excel_custo(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base

    gen_df = df_excel_custo(file_in)

    dataset = 'dev_domestico'

    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_df_custo(gen_df, dataset, "custo_2021_excel")

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    #q_comparativo = "custo_comparativo.sql"
    #execute_qery(q_comparativo)

def convert_excel_recebido(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base

    gen_df = df_excel_recebido(file_in)

    dataset = 'dev_domestico'

    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_df_recebido(gen_df, dataset, "recebido_2021_excel")

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    #q_comparativo = "custo_comparativo.sql"
    #execute_qery(q_comparativo)    


def convert_excel_saldo(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base

    gen_df = df_excel_saldo(file_in)

    dataset = 'dev_domestico'

    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_df_saldo(gen_df, dataset, "saldo_2021_excel")

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    #q_comparativo = "custo_comparativo.sql"
    #execute_qery(q_comparativo)    

def convert_excel_credito(base):

    #--> definir arquivo que sera gerado no storage para depois converter em tabela
    file_in = base

    gen_df = df_excel_credito(file_in)

    dataset = 'dev_domestico'

    #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
    InsertBq().insert_df_credito(gen_df, dataset, "credito_2021_excel")

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    #q_comparativo = "custo_comparativo.sql"
    #execute_qery(q_comparativo)      

def execute_qery(queri):

    #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
    MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'sql/domestico/', 'sql/', queri)

    #--> pegar arquivo no storage - parametros: nome do bucket, diretorio do bucket, arquivo
    queri_comparativo = MovimentacaoFileBucket().pull_file('proj-domestico-file', 'sql/', queri)

    #--> executar query
    ret_comparativo = InsertBq().create_job(queri_comparativo)
    print(ret_comparativo)

def execute():
    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_comparativo = "tab_custo_comparativo.sql"
    execute_qery(q_comparativo)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_consolidado = "tab_custo_consolidado.sql"
    execute_qery(q_consolidado)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_recebdio = "tab_recebido_previsao.sql"
    execute_qery(q_recebdio)

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_saldo = "tab_saldo_gerado_mes_base.sql"
    execute_qery(q_saldo)   

    #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
    q_credito = "tab_credito.sql"
    execute_qery(q_credito)    

def gerar_credito():
    for i in range(8,13):
        if i <= 9:
            print(f'credito_2021_0{i}')
            file = f'credito_2021_0{i}'
        else:
            print(f'credito_2021_{i}')
            file = f'credito_2021_{i}'
        convert_excel_credito(file+'.xls')


def gerar_debito():
    for i in range(7,9):
        if i <= 9:
            print(f'custo_2021_0{i}')
            file = f'custo_2021_0{i}'
        else:
            print(f'custo_2021_{i}')
            file = f'custo_2021_{i}'
        #convert_excel_credito(file+'.xls')
        convert_excel_custo(file+'.xls')
        convert_excel_recebido(file+'.xls')
        convert_excel_saldo(file+'.xls')    


'''
file='custo_2021_09'
convert_excel_custo(file+'.xls')
convert_excel_recebido(file+'.xls')
convert_excel_saldo(file+'.xls')  
gerar_credito()
execute()

q_credito = "tab_credito.sql"
execute_qery(q_credito)  
'''

execute()
  