#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd 
import psycopg2
import psycopg2.extras as extras

#Extrair dados da API
def extract():
    request = requests.get("http://****:8000/read")

    if request.status_code == 200:
        dados = request.json()
        
        dicionario_dados = {
                "ID_CLIENTE": [item.get("id") for item in dados],
                "NOME_CLIENTE": [item.get("nome") for item in dados],
                "IDADE_CLIENTE": [item.get("idade") for item in dados],
                "EMAIL_CLIENTE": [item.get("email") for item in dados],
                "TELEFONE_CLIENTE": [item.get("telefone") for item in dados],
                "LOGRADOURO_CLIENTE": [item["endereco"].get("logradouro") for item in dados],
                "NUMERO_RESIDENCIA_CLIENTE": [item["endereco"].get("numero") for item in dados],
                "BAIRO_CLIENTE": [item["endereco"].get("bairro") for item in dados],
                "CIDADE_CLIENTE": [item["endereco"].get("cidade") for item in dados],
                "ESTADO_CLIENTE": [item["endereco"].get("estado") for item in dados],
                "CEP_CLIENTE": [item["endereco"].get("cep") for item in dados]
        }
        df = pd.DataFrame(dicionario_dados)
        return df
    else:
        print("Falha ao acessar a API")
        return None
    
#Cria e trunca a tabela CLIENTE
def create_table_stage(conn):
    cur = conn.cursor() 
    try:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.CLIENTE (
                        ID_CLIENTE                  INTEGER PRIMARY KEY,
                        NOME_CLIENTE                VARCHAR(100),
                        IDADE_CLIENTE               INTEGER,
                        EMAIL_CLIENTE               VARCHAR(100),
                        TELEFONE_CLIENTE            VARCHAR(20),
                        LOGRADOURO_CLIENTE          VARCHAR(100),
                        NUMERO_RESIDENCIA_CLIENTE   INTEGER,
                        BAIRO_CLIENTE               VARCHAR(100),
                        CIDADE_CLIENTE              VARCHAR(100),
                        ESTADO_CLIENTE              VARCHAR(2),
                        CEP_CLIENTE                 VARCHAR(9),
                        DT_ULT_ATUALIZACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    TRUNCATE TABLE public.CLIENTE;
        """)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 
        conn.rollback()
    else:
        conn.commit()

#Insere os dados extraídos da API para o Stage
def insert_values_stage(conn, df):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns)) 
    query = """INSERT INTO public.CLIENTE(%s) VALUES %%s;""" % (cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    cursor.close()


def main():
    conn_stage = psycopg2.connect(
        host="****",
        port=5433,
        database="Stage",
        user="capim",
        password="capim")
    

    print("Extraindo dados...")    
    data = extract()

    print("Carregando dados no Stage...")
    create_table_stage(conn_stage)
    insert_values_stage(conn_stage, data)

    print("Finalizado.")

if __name__ == "__main__":
    main()