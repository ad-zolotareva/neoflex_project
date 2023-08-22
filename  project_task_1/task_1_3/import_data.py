from io import open
import pandas as pd
import psycopg2
import param_connect_with_db as cdb

def sql_query(conn,query):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()


def data_handling(conn):
    number = input("\n\n1 - выгрузить данные из витрины «dm.dm_f101_round_f» в csv-файл\n"
                   "2 - загрузить данные в копию таблицы 101-формы «dm.dm_f101_round_f_v2\n"
                   "Введите номер действия:"
                   )

    match number:
        case "1":
            print("\nОчищается csv-файл перед загрузкой данных")
            with open('/home/dm_f101_round_f.csv', "w+") as f:
                sql_query(conn,
                    '''COPY dm.dm_f101_round_f TO '/home/dm_f101_round_f.csv' DELIMITER ';' CSV HEADER NULL '';''')
            print("\nДанные в csv-файл выгружены")

        case "2":
            dm_f101_round_f_file_name = "/home/dm_f101_round_f.csv"

            sql_query(conn, '''DROP TABLE IF EXISTS dm.dm_f101_round_f_v2;''')
            sql_query(conn,
                '''CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 AS (SELECT * FROM dm.dm_f101_round_f) with no data;''')
            sql_query(conn,
                '''insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу dm_f101_round_f_v2',  LOCALTIMESTAMP(0));''')
            sql_query(conn,'''SET search_path TO dm, public;''')

            cur = conn.cursor()
            with open(dm_f101_round_f_file_name) as f:
                next(f)
                tbl = 'dm_f101_round_f_v2'
                sep = ';'
                cur.copy_from(f, tbl, sep, null='')
            sql_query(conn,
                '''insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу dm_f101_round_f_v2',  LOCALTIMESTAMP(0));''')

            print("\nДанные в таблицу выгружены")
        case _:
            print("----------Такого действия нет----------")


def import_date_start():
    conn = psycopg2.connect(host=cdb.host, database=cdb.dbname, user=cdb.user, password=cdb.password)
    data_handling(conn)
    answer = "1"
    while answer == "1":
        answer = input("\nХотите продолжить? (1 - да; прочее - нет) ")
        match answer:
            case "1":
                data_handling(conn)
            case _:
                print("")

    conn.close()