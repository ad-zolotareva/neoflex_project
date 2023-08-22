import param_connect_with_db as cdb
import pandas as pd
import time
import psycopg2


def sql_query(conn, query, parameters=''):
    cur = conn.cursor()
    if parameters == '':
        cur.execute(query)
    else:
        cur.execute(query, parameters)
    conn.commit()
    cur.close()


def table_printing(conn, str):
    cursor = conn.cursor()
    postgresql_select_query = str
    print(f'\n {str}\n')
    cursor.execute(postgresql_select_query)
    while cursor.fetchone() != None:
        records = cursor.fetchone()
        print(records)
    cursor.close()


def showcase_calculation():
    conn = psycopg2.connect(host=cdb.host, database=cdb.dbname, user=cdb.user, password=cdb.password)
    sql_query(conn, '''DELETE FROM dm.dm_account_turnover_f''')
    sql_query(conn, '''DELETE FROM dm.dm_f101_round_f''')
    sql_query(conn, '''DELETE FROM cron.job_run_details''')
    sql_query(conn,
              '''SELECT cron.schedule('process-my_procedure', '* * * * *', 'CALL dm.my_procedure(to_date(
              ''2018.01.01'',''yyyy-mm-dd''),to_date(''2018.01.31'',''yyyy-mm-dd''))');''')
    table_printing(conn, "SELECT * FROM cron.job;")
    time.sleep(65)
    table_printing(conn, "SELECT * FROM dm.dm_account_turnover_f;")
    table_printing(conn, "SELECT * FROM cron.job_run_details ORDER BY start_time DESC;")
    sql_query(conn, '''SELECT cron.unschedule('process-my_procedure');''')
    sql_query(conn, '''SELECT cron.schedule('process-fill_f101_round_f', '* * * * *', 'CALL  dm.fill_f101_round_f(to_date(
                  ''2018.01.01'', ''yyyy.mm.dd''))');''')
    time.sleep(65)
    table_printing(conn, "SELECT * FROM dm.dm_f101_round_f;")
    table_printing(conn, "SELECT * FROM cron.job_run_details ORDER BY start_time DESC;")
    sql_query(conn, '''SELECT cron.unschedule('process-fill_f101_round_f');''')
    conn.close()
