# Programa para carga de topico no kafka
# Autor: Fernando Alves da Costa
# Data Criação: 14/01/2021

# Bibliotecas utilizadas

from kafka import KafkaConsumer
from json import loads
import psycopg2
import ast
import sys
import datetime

# Tratando os parametros de entrada
param1=[]
param1=str(' '.join(sys.argv[1:])).split()

if param1 == '':
    print("Favor, passar o parametro com o nome do tópico")
    sys.exit()
else:
    print("Parametro encontrado")

# Parametros de banco de dados
host = "postgres02"
dbname = "dw"
user = "dw"
password = "Dw01"
port = "5432"

# inicio do loop, o programa foi feito para passar varios tópicos como entrada
for param in param1:
    print("Processando o topico: ",param)

    # Conexao com o banco de dados
    conn_string = "host={0} user={1} dbname={2} password={3} port={4}".format(host, user, dbname, password, port)
    conn = psycopg2.connect(conn_string)
    try:
        print("Sucesso ao conectar no banco de dados")
    except:
        print("Erro ao conectar no banco de dados")
        sys.exit()

    # Conexao com o kafka com metódo consumer
    # Para consumir o tópico desde o inicio, alterar o parametro auto_offset_reset='earliest'
    # Para nao deixar o programa rodando sem parar, incluir o parametro consumer_timeout_ms = 15000
    consumer = KafkaConsumer(param, bootstrap_servers='kafka01:9092', consumer_timeout_ms = 15000, auto_offset_reset='latest', enable_auto_commit=True)
    try:
        print("Sucesso ao conectar no kafka")
    except:
        print("Erro ao conectar no kafka")
        sys.exit()
    # Verifica qual o tópico, para definir tabela no postgres

    def verifica_layout_arquivo():
        global transaction_id_
        global account_id_
        global counterparty_account_id_
        global amount_
        global inserted_at_
        global postgres_insert_query
        global record_to_insert
        
        # Preenchendo as variaveis para carga no postgres

        if param == 'cash_ins':
            transaction_id_=mydata.get('transaction_id')
            account_id_=mydata.get('account_id')
            counterparty_account_id_=mydata.get('counterparty_account_id')
            amount_=mydata.get('amount')
            inserted_at_=datetime.datetime.fromtimestamp(mydata.get('inserted_at') / 1e3)
            postgres_insert_query = """ insert into cash_ins (transaction_id, account_id, counterparty_account_id, amount, inserted_at) values (%s, %s, %s, %s, %s)"""
            record_to_insert = (transaction_id_, account_id_, counterparty_account_id_, amount_, inserted_at_)
        elif param == 'cash_outs_created':
            transaction_id_=mydata.get('transaction_id')
            account_id_=mydata.get('account_id')
            counterparty_account_id_=mydata.get('counterparty_account_id')
            amount_=mydata.get('amount')
            inserted_at_=datetime.datetime.fromtimestamp(mydata.get('inserted_at') / 1e3)
            postgres_insert_query = """ insert into cash_outs_created (transaction_id, account_id, counterparty_account_id, amount, inserted_at) values (%s, %s, %s, %s, %s)"""
            record_to_insert = (transaction_id_, account_id_, counterparty_account_id_, amount_, inserted_at_)
        elif param == 'cash_outs_refunded':
            transaction_id_=mydata.get('transaction_id')
            inserted_at_=datetime.datetime.fromtimestamp(mydata.get('inserted_at') / 1e3)
            postgres_insert_query = """ insert into cash_outs_refunded (transaction_id, inserted_at) values (%s, %s)"""
            record_to_insert = (transaction_id_, inserted_at_)
        elif param == 'cash_outs_processed':
            transaction_id_=mydata.get('transaction_id')
            inserted_at_=datetime.datetime.fromtimestamp(mydata.get('inserted_at') / 1e3)
            postgres_insert_query = """ insert into cash_outs_processed (transaction_id, inserted_at) values (%s, %s)"""
            record_to_insert = (transaction_id_, inserted_at_)
        else:
            print("Verificar parametro")
            sys.exit()

    contador=0
    
    # leitura do conteudo extraido do kafka
    for message in consumer:
        linha=message.value
        dict_str = linha.decode("utf-8")
        mydata = ast.literal_eval(dict_str)

    # Carregando os registros no postgres
        verifica_layout_arquivo()
        cur = conn.cursor()
        cur.execute(postgres_insert_query, record_to_insert)
        cur.close()
        conn.commit()
        
        contador = contador + 1

    print("Registros carregados: ",contador)

consumer.close()
conn.close()

