import psycopg2
from psycopg2 import Error

def create_server_conection(host_name, user_name, user_password):   #Função que estabelece conexão com o SQL | pode adicionar "db_name" como parâmetro
    connection = None   #Encerra qualquer conexão remanescente
    try:
        connection = psycopg2.connect(
            host = host_name,
            user = user_name,
            password = user_password
        ) 
        print("Conexão estabelecida com sucesso!!!")
    except Error as err:
        print(f"Error: {err}")
    return connection

connection = create_server_conection("localhost", "postgres", "quickscope1")
connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT) #indica o fim normal da transação - trecho para resolver erro

def create_database(connection, query):  #Função quer cria um banco de dados
    cursor = connection.cursor()
    try:
        cursor.execute(query) #Dá acesso ao cursor que fica piscando em um terminal do SQL
        print("Database criada com sucesso!!!")
    except Error as err:
        print(f"Error: {err}")

create_database_query = "CREATE DATABASE books_data"
create_database(connection, create_database_query)