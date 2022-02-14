from bq.insert_bq import InsertBq
from bucket.mov_file_bucket import MovimentacaoFileBucket
from pandadf.generate_df_excel import GenerateDF
from resource.var import ExportVar

class Custo():
        
    def df_excel_custo(self, file_in):

        #--> return df
        df = GenerateDF().excel_generate_df_custo('csv/domestico/excel/'+file_in)
        #excel_generate_df_recebido

        #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
        return df


    def df_excel_credito(self, file_in):

        #--> return df
        df = GenerateDF().excel_generate_df_credito('csv/domestico/excel/'+file_in)
        #excel_generate_df_recebido

        #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
        return df

    def df_excel_recebido(self, file_in):

        #--> return df
        df = GenerateDF().excel_generate_df_recebido('csv/domestico/excel/'+file_in)
        #excel_generate_df_recebido

        #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
        return df

    def df_excel_saldo(self, file_in):

        #--> return df
        df = GenerateDF().excel_generate_df_saldo('csv/domestico/excel/'+file_in)
        #excel_generate_df_recebido

        #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
        return df    

    def df_excel_listacompras(self, file_in):

        #--> return df
        df = GenerateDF().excel_generate_df_listacompras('csv/domestico/excel/'+file_in)
        #excel_generate_df_recebido

        #df = GenerateDF().csv_generate_df_custo('csv/domestico/'+file_in)
        return df    


    def convert_excel_custo(self, base, ano):

        #--> definir arquivo que sera gerado no storage para depois converter em tabela
        file_in = base

        gen_df = Custo().df_excel_custo(file_in)

        dataset = 'dev_domestico'

        #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
        InsertBq().insert_df_custo(gen_df, dataset, f"custo_{ano}excel")

        #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        #q_comparativo = "custo_comparativo.sql"
        #execute_qery(q_comparativo)

    def convert_excel_recebido(self, base, ano):

        #--> definir arquivo que sera gerado no storage para depois converter em tabela
        file_in = base

        gen_df = Custo().df_excel_recebido(file_in)

        dataset = 'dev_domestico'

        #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
        InsertBq().insert_df_recebido(gen_df, dataset, f"recebido_{ano}excel")

        #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        #q_comparativo = "custo_comparativo.sql"
        #execute_qery(q_comparativo)    


    def convert_excel_saldo(self, base, ano):

        #--> definir arquivo que sera gerado no storage para depois converter em tabela
        file_in = base

        gen_df = Custo().df_excel_saldo(file_in)

        dataset = 'dev_domestico'

        #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
        InsertBq().insert_df_saldo(gen_df, dataset, f"saldo_{ano}excel")

        #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        #q_comparativo = "custo_comparativo.sql"
        #execute_qery(q_comparativo)    

    def convert_excel_credito(self, base, ano):

        #--> definir arquivo que sera gerado no storage para depois converter em tabela
        file_in = base

        gen_df = Custo().df_excel_credito(file_in)

        dataset = 'dev_domestico'

        #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
        InsertBq().insert_df_credito(gen_df, dataset, f"credito_{ano}excel")

        #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        #q_comparativo = "custo_comparativo.sql"
        #execute_qery(q_comparativo)      

    def convert_excel_listacompras(self, base):

        #--> definir arquivo que sera gerado no storage para depois converter em tabela
        file_in = base

        gen_df = Custo().df_excel_listacompras(file_in)

        dataset = 'dev_domestico'

        #--> gerar tabela no Bigquery baseado no arquivo do storage - parametros: dataset, tabela, arquivo
        InsertBq().insert_df_listacompras(gen_df, dataset, "lista_compras")

        #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        #q_comparativo = "custo_comparativo.sql"
        #execute_qery(q_comparativo)  

    def execute_qery(self, queri):

        #--> upload do arquivo no storage - parametros: nome do bucket, diretorio local do arquivo, diretorio do bucket, arquivo
        MovimentacaoFileBucket().upload_blob('proj-domestico-file', 'sql/domestico/', 'sql/', queri)

        #--> pegar arquivo no storage - parametros: nome do bucket, diretorio do bucket, arquivo
        queri_comparativo = MovimentacaoFileBucket().pull_file('proj-domestico-file', 'sql/', queri)

        #--> executar query
        ret_comparativo = InsertBq().create_job(queri_comparativo)
        print(ret_comparativo)

    def execute(self):
        #--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        #q_comparativo = "tab_custo_comparativo.sql"
        #Custo().execute_qery(q_comparativo)

        #INCLUIDO 2022--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        q_consolidado = "tab_estrat_custo_consolidado_2022.sql"
        Custo().execute_qery(q_consolidado)

        #INCLUIDO 2022--> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        q_credito = "tab_credito_2022.sql"
        Custo().execute_qery(q_credito)    

        #EM CONSTRUÇÃO--> lista de itens de mercado - CRIAR MAIS UMA TABELA QUE CRUZA DADOS PREVISÃO E HISTORICO
        q_lista = "tab_lista_produtos_mais_consumidos.sql"
        Custo().execute_qery(q_lista)    

        #EM CONSTRUÇÃO --> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        q_recebdio = "tab_recebimento_geral_2022.sql"
        Custo().execute_qery(q_recebdio)

        #EM CONSTRUÇÃO --> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        q_saldo = "tab_saldo_2022.sql"
        Custo().execute_qery(q_saldo)

        #EM CONSTRUÇÃO --> definir arquivo SQL que sera gerado no storage / depois executar no bigquery / gerar tabela
        q_custo_vida = "tab_custo_vida_geral_2022.sql"
        Custo().execute_qery(q_custo_vida)        #

    def gerar_credito(self, file, ano):
        for i in range(1,13):
            if i <= 9:
                print(f'{file}{ano}0{i}')
                filex= f'{file}{ano}0{i}'
            else:
                print(f'{file}{ano}{i}')
                filex = f'{file}{ano}{i}'
            Custo().convert_excel_credito(filex+'.xls', ano)


    def gerar_debito(self, file, ano):
        for i in range(12,13):
            if i <= 9:
                print(f'{file}{ano}0{i}')
                filex= f'{file}{ano}0{i}'
            else:
                print(f'{file}{ano}{i}')
                filex = f'{file}{ano}{i}'
            #convert_excel_credito(file+'.xls')
            Custo().convert_excel_custo(filex+'.xls', ano)
            Custo().convert_excel_recebido(filex+'.xls', ano)
            Custo().convert_excel_saldo(filex+'.xls', ano)    


'''
ExportVar().variavel()

file='custo_2021_10'
convert_excel_custo(file+'.xls')
convert_excel_recebido(file+'.xls')
convert_excel_saldo(file+'.xls')  
gerar_credito()


file='lista_compras'
convert_excel_listacompras(file+'.xls')

execute()


file='custo_'
ano ='2022_'
Custo().convert_excel_custo(f'{file}{ano}01.xls', ano)
Custo().convert_excel_recebido(f'{file}{ano}01.xls', ano)
Custo().convert_excel_saldo(f'{file}{ano}01.xls', ano)

file='credito_'
Custo().gerar_credito(file, ano)

file='lista_compras'
Custo().convert_excel_listacompras(file+'.xls')

Custo().execute()

'''
ExportVar().variavel()

Custo().execute()