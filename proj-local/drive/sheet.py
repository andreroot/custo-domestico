
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd

from google.oauth2 import service_account

# use creds to create a client to interact with the Google Drive API

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive",
          "https://spreadsheets.google.com/feeds"]
json_file = "key.json"


class DriveSheet():
    def login():

        credentials = service_account.Credentials.from_service_account_file('resource/'+json_file)
        scoped_credentials = credentials.with_scopes(scopes)
        gc = gspread.authorize(scoped_credentials)
        return gc 

    def leitor(self):
        gc = DriveSheet.login()
        planilha = gc.open("recebidos").sheet1
        print(planilha.get('A1'))
        #aba = planilha.worksheet("Respostas ao formulário 1")
        dados = planilha.get_all_records()
        #df = pd.DataFrame(dados)
        #print(df)
        return dados
