# custo-domestico
desenvolver um processo de analise de custo em python



**Projeto esta baseado em query que montam um fluxo de entrada e saida e cruzam dados**

- os dados de entrada, saida e credito, estão concentrados nos extratos baixados periodicamente do banco e gerados em excel
- lista de compras são imagens de extratos gravados no google e gerados arquivos txt que são convertidos em excel
- processo ler o excel e gera dataframe que serão gerados em tabela sno bigquery
- os arquivos sql serão armazenadas no storage



# 1.1 planejamento de custo

lista de query 

a. sql/domestico/tab_estrat_custo_consolidado_2022

DADOS DE CUSTO

SCHEMA



| tabela envolvidas                         |      |
| ----------------------------------------- | ---- |
| devsamelo2.dev_domestico.custo_2021_excel |      |
| devsamelo2.dev_domestico.custo_2022_excel |      |
| devsamelo2.dev_domestico.custo_forms      |      |

montar tabela em forms

CUSTO: https://docs.google.com/forms/d/1M4VQ-EhfrF1A3pkGWNObKA-2fp2ks9-e7AADPw3YnCA/edit
SHEET: https://docs.google.com/spreadsheets/d/165LxRPISVoidWCXTPw7cCKnMAOO0p0zzNvJshET_5Qk/edit?resourcekey#gid=2013346928
TABELA BQ(tabelas gerada no bigqury que estão espelhando os dados):devsamelo2:dev_domestico.custo_forms_2021

fonte: https://docs.google.com/spreadsheets/d/165LxRPISVoidWCXTPw7cCKnMAOO0p0zzNvJshET_5Qk/edit?resourcekey#gid=2013346928

colunas:
intervale de colunas dados!B2:J100
data_base_bq	DATE	NULLABLE	
custo	        STRING	NULLABLE	
tipo_custo	    STRING	NULLABLE	
dt_custo_bq	    DATE	NULLABLE	
valor_custo	    FLOAT	NULLABLE	
ano_base	    INTEGER	NULLABLE	
mes_base_ordem	INTEGER	NULLABLE	
mes_base	    STRING	NULLABLE	
pendente        STRING  NULLABLE

data_base_bq:DATE,custo:STRING,tipo_custo:STRING,dt_custo_bq:DATE,valor_custo:FLOAT,ano_base:INTEGER,mes_base_ordem:INTEGER,mes_base:STRING,pendente:STRING

# 1.2 controlar entrada de recurso *em construção

lista de query

a. sql/domestico/tab_recebido_previsao_2022

DADOS DE ENTRADA DE RECURSO

SCHEMA

| tabela envolvidas                         |      |
| ----------------------------------------- | ---- |
| devsamelo2.dev_domestico.custo_2021_excel |      |
| devsamelo2.dev_domestico.custo_2022_excel |      |
| devsamelo2.dev_domestico.custo_forms      |      |

montar tabela forms

RECEBIDO: https://docs.google.com/forms/d/1IIEZPOCbjATyrHYA8vKltr4rvFwcVmKKGf_V3gjmKBw/edit
SHEET: https://docs.google.com/spreadsheets/d/1ICwGSx2d3LEx_IlO1mJFZrHesss-qTM9SjCCi0XpaPU/edit?resourcekey#gid=963707413
TABELA BQ(tabelas gerada no bigqury que estão espelhando os dados):devsamelo2:dev_domestico.recebido_forms_2021



dt_mes_base:DATE,descricao:STRING,valor_recebido:FLOAT,dt_recebido:DATE


# 1.3 acompanhar gasto com cartão de credito

lista de query

a. sql/domestico/tab_credito_2022

DADOS DE GASTO DO CARTÃO DE CREDITO

SCHEMA

| tabela envolvidas                           |      |
| ------------------------------------------- | ---- |
| devsamelo2.dev_domestico.credito_2021_excel |      |
| devsamelo2.dev_domestico.credito_2022_excel |      |
| devsamelo2.dev_domestico.custo_forms        |      |

# 1.4  previsão da lista de compras do mercado

lista de query

a. sql/domestico/tab_lista_produtos_mais_consumidos

DADOS DE LISTA DE ITENS COMPRADOS NO MERCADO

SCHEMA



| tabela envolvidas                      |      |
| -------------------------------------- | ---- |
| devsamelo2.dev_domestico.lista_compras |      |




# 2.1 configurar drive para tabelas forms

rodar query tabela externa no google drive:

export GOOGLE_APPLICATION_CREDENTIALS='/home/andre/Documents/b2w/particular/github/custo-domestico/proj-local/resource/key.json'

cofigurar: https://cloud.google.com/bigquery/external-data-drive#enable-google-drive

        credentials, project = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
    
        # Construct a BigQuery client object.
        client = bigquery.Client(credentials=credentials, project='devsamelo2')

