
import csv, sys, os

class ConvertCsv():

    def ler(filein):
        
        #--> abrir arquivo para ler
        lista =[]
        with open(filein, 'rt') as ficheiro:
            reader = csv.reader(ficheiro)
            try:
                for  linha in reader:
                    
                lista.append(linha)

                return lista

            except csv.Error as e:
                sys.exit('ficheiro %s, linha %d: %s' % (filein, reader.line_num, e))