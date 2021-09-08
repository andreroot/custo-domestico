# custo-domestico
desenvolver um processo de analise de custo em python

# 1.1 registrar custo

baixar extrato em txt do site do itau
ler os campos e gerar um dataframe

# 1.1.a registrar as origens dos custos

no dataframe, criar um campo de grupo de custo

# 1.1.b converter campos data e valores no formato ideal

gravar numa tabela

# 1.2 registrar entrada

# 1.3 previsões custo e entrada

# 1.3.1 RECEBIDO

entrada via forms:

RECEBIDO: https://docs.google.com/forms/d/1IIEZPOCbjATyrHYA8vKltr4rvFwcVmKKGf_V3gjmKBw/edit
SHEET: https://docs.google.com/spreadsheets/d/1ICwGSx2d3LEx_IlO1mJFZrHesss-qTM9SjCCi0XpaPU/edit?resourcekey#gid=963707413
TABELA BQ(tabelas gerada no bigqury que estão espelhando os dados):devsamelo2:dev_domestico.recebido_forms_2021

# 1.3.1 CUSTO

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

# 2.1 configurar drive

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


