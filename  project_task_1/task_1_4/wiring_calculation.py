import psycopg2
import time
import datetime
from datetime import date
import param_connect_with_db as cdb


def sql_query(conn, query, parameters=''):
    cur = conn.cursor()
    if parameters == '':
        cur.execute(query)
    else:
        cur.execute(query, parameters)
    conn.commit()
    cur.close()

def input_date(str):
    print(f"\n----------Введите {str} дату----------")
    p_year = input("\nГод ")
    p_month = input("\nМесяц ")
    p_day = input("\nДень ")
    return date(int(p_year), int(p_month), int(p_day))

def input_dates():
    is_not_correct = True
    while is_not_correct:
        try:
            date_1 = input_date('начальную')
            date_2 = input_date('конечную')
            is_not_correct = False
            return date_1, date_2
        except:
            print("----------Некорректные данные-----------")


def wiring_calculation_start(date_s=None, date_f=None):
    conn = psycopg2.connect(host=cdb.host, database=cdb.dbname, user=cdb.user, password=cdb.password)

    date_s, date_f = input_dates()
    is_not_correct = True
    while is_not_correct:
        if date_s > date_f:
            print("----------Конечная дата меньше начальной, повторите ввод----------")
            date_s, date_f = input_dates()
        else:
            is_not_correct = False

    one_day = datetime.timedelta(days=1)

    with open('/home/posting_file.csv', "w+") as f:
        sql_query(conn,
                  f'''COPY (select * from debit_and_credit_posting(to_date('{date_s}','yyyy-mm-dd'))) TO PROGRAM  'cat >>/home/posting_file.csv' DELIMITER ';' CSV HEADER NULL '';''')
        date_s = date_s + one_day

        while date_s <= date_f:
            sql_query(conn,
                      f'''COPY (select * from debit_and_credit_posting(to_date('{date_s}','yyyy-mm-dd'))) TO PROGRAM  'cat >>/home/posting_file.csv' DELIMITER ';' NULL '';''')
            date_s = date_s + one_day
    print("\nДанные в csv-файл выгружены")

    conn.close()
