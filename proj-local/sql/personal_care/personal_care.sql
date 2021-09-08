SELECT * 
FROM `devsamelo2.dev_personal_care.plantao_forms` 

#Carimbo de data/hora(A)	data_plantao(B)	hora_plantao	local_plantao	nome_paciente	pendente?	definir_cadastro?
#https://docs.google.com/spreadsheets/d/1-0_LO5aQOVDFMQbhkd6-0VyfqOlvXputJ7eOEEhDDLw/edit?resourcekey#gid=593346448
#dados!A2:H100
#data_forms:TIMESTAMP,data_plantao:DATE,hora_plantao:STRING,local_plantao:STRING,nome_paciente:STRING,horas_pendeten:STRING,cad_source:STRING
data_forms:TIMESTAMP,data_plantao:DATE,hora_plantao:STRING,local_plantao:STRING,nome_paciente:STRING,horas_pendeten:STRING,cad_source:STRING, valor_plantao:FLOAT