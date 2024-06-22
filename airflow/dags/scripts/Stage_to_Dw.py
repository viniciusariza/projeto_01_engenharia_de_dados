#!/usr/bin/env python
# coding: utf-8

import psycopg2
from datetime import datetime

#Cria a tabela DIM_CLIENTE
def create_table_dw(conn):
    cur = conn.cursor() 
    try:
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.DIM_CLIENTE (
                        SURROGATE_ID_CLIENTE        SERIAL PRIMARY KEY,
                        ID_CLIENTE                  INTEGER,
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
                        DT_INI                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        DT_FIM                      TIMESTAMP
                    );
        """)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 
        conn.rollback()
    else:
        conn.commit()

#Insere os dados do Stage no DW, aplicando SCD tipo 2
def insert_values_dw(conn_stage, conn_dw):
    consulta = """SELECT 
                    ID_CLIENTE,
                    NOME_CLIENTE,
                    IDADE_CLIENTE,
                    EMAIL_CLIENTE,
                    TELEFONE_CLIENTE,
                    LOGRADOURO_CLIENTE,
                    NUMERO_RESIDENCIA_CLIENTE,
                    BAIRO_CLIENTE,
                    CIDADE_CLIENTE,
                    ESTADO_CLIENTE,
                    CEP_CLIENTE
                FROM public.CLIENTE
        """

    try:
        cursor_stage = conn_stage.cursor()
        cursor_stage.execute(consulta)
        stage_data = cursor_stage.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error fetching data from Stage table: %s" % error)
        return 1

    insert_query = """
        INSERT INTO public.DIM_CLIENTE (
            ID_CLIENTE,
            NOME_CLIENTE,
            IDADE_CLIENTE,
            EMAIL_CLIENTE,
            TELEFONE_CLIENTE,
            LOGRADOURO_CLIENTE,
            NUMERO_RESIDENCIA_CLIENTE,
            BAIRO_CLIENTE,
            CIDADE_CLIENTE,
            ESTADO_CLIENTE,
            CEP_CLIENTE,
            DT_INI,
            DT_FIM
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor_dw = conn_dw.cursor()
        current_date = datetime.now()
        
        for row in stage_data:
            id_cliente = row[0]
            # Verifica se já existe um registro com o mesmo ID_CLIENTE na DIM_CLIENTE
            cursor_dw.execute("SELECT * FROM public.DIM_CLIENTE WHERE ID_CLIENTE = %s AND DT_FIM IS NULL", (id_cliente,))
            existing_record = cursor_dw.fetchone()
            if existing_record:
                # Se o registro existir, verifica se algum campo foi alterado
                if existing_record[2:-2] != row[1:]:
                    # Se algum campo foi alterado, atualizar a data de término do registro existente
                    cursor_dw.execute("UPDATE public.DIM_CLIENTE SET DT_FIM = %s WHERE SURROGATE_ID_CLIENTE = %s", (current_date, existing_record[0]))
                    # Inserir o novo registro com a data de início como a data atual
                    cursor_dw.execute(insert_query, list(row) + [current_date, None])
            else:
                # Se o registro não existir, inserir o novo registro com a data de início como a data atual
                cursor_dw.execute(insert_query, list(row) + [current_date, None])

        # Consulta para recuperar os IDs de cliente da tabela public.CLIENTE
        cursor_stage.execute("SELECT ID_CLIENTE FROM public.CLIENTE")
        client_ids_stage = [row[0] for row in cursor_stage.fetchall()]

        # Marcar como terminados os registros na DIM_CLIENTE que não estão mais na tabela public.CLIENTE
        cursor_dw.execute("""
            UPDATE public.DIM_CLIENTE 
            SET DT_FIM = %s 
            WHERE ID_CLIENTE NOT IN %s
            AND DT_FIM IS NULL
        """, (current_date, tuple(client_ids_stage)))

        conn_dw.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error inserting data into data warehouse: %s" % error)
        conn_dw.rollback()
        return 1
    finally:
        cursor_dw.close()
        cursor_stage.close()


def main():
    conn_stage = psycopg2.connect(
        host="****",
        port=5433,
        database="Stage",
        user="capim",
        password="capim")
    
    conn_dw = psycopg2.connect(
        host="****",
        port=5433,
        database="DataWarehouse",
        user="capim",
        password="capim")

    print("Carregando dados no DataWarehouse...")
    create_table_dw(conn_dw)
    insert_values_dw(conn_stage, conn_dw)

    print("Finalizado.")

if __name__ == "__main__":
    main()