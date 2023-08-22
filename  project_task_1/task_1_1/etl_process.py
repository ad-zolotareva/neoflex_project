import psycopg2
import time
import task_1_1.rows_table as rows_table
import param_connect_with_db as cdb


def sql_query(conn, query, parameters=''):
    cur = conn.cursor()
    if parameters == '':
        cur.execute(query)
    else:
        cur.execute(query, parameters)
    conn.commit()
    cur.close()


def logging_start(conn, name_table):
    sql_query(conn,
        f"insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу {name_table}',  LOCALTIMESTAMP(0));")


def end_of_logging(conn, name_table):
    time.sleep(5)
    sql_query(conn,
        f"insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу {name_table}',  LOCALTIMESTAMP(0));")


def data_loading(conn):
    logging_start(conn, 'FT_BALANCE_F')
    for index, row in rows_table.balance.iterrows():
        row_balance = (
            row['ON_DATE'], row['ACCOUNT_RK'], row['CURRENCY_RK'], row['BALANCE_OUT'], row['CURRENCY_RK'],
            row['BALANCE_OUT'])
        sql_query(conn,
            "insert into DS.FT_BALANCE_F values ( %s,%s, %s,%s) ON CONFLICT (on_date,account_rk) DO UPDATE SET CURRENCY_RK = %s, BALANCE_OUT = %s;",
            row_balance)
    end_of_logging(conn, 'FT_BALANCE_F')
    print("Данные в таблицу FT_BALANCE_F  добавлены", )

    logging_start(conn, 'MD_ACCOUNT_D')
    for index, row in rows_table.account.iterrows():
        row_account = (
            row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['ACCOUNT_RK'], row['ACCOUNT_NUMBER'],
            row['CHAR_TYPE'],
            row['CURRENCY_RK'], row['CURRENCY_CODE'], row['DATA_ACTUAL_END_DATE'], row['ACCOUNT_NUMBER'],
            row['CHAR_TYPE'],
            row['CURRENCY_RK'], row['CURRENCY_CODE'])
        sql_query(conn,
            "insert into DS.MD_ACCOUNT_D values ( %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (data_actual_date,account_rk) DO UPDATE SET DATA_ACTUAL_END_DATE = %s,ACCOUNT_NUMBER = %s, CHAR_TYPE = %s, CURRENCY_RK = %s, CURRENCY_CODE = %s;",
            row_account)
    end_of_logging(conn,'MD_ACCOUNT_D')
    print("Данные в таблицу MD_ACCOUNT_D добавлены")

    logging_start(conn,'MD_CURRENCY_D')
    for index, row in rows_table.currency.iterrows():
        row_currency = (row['CURRENCY_RK'], row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_CODE'],
                        row['CODE_ISO_CHAR'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_CODE'], row['CODE_ISO_CHAR'])
        sql_query(conn,
            "insert into DS.MD_CURRENCY_D values ( %s, %s, %s, %s, %s) ON CONFLICT (currency_rk,data_actual_date) DO UPDATE SET DATA_ACTUAL_END_DATE = %s, CURRENCY_CODE = %s, CODE_ISO_CHAR = %s;",
            row_currency)
    end_of_logging(conn,'MD_CURRENCY_D')
    print("Данные в таблицу MD_CURRENCY_D добавлены")

    # Для избавления от дубляжей, создаем временную таблицу, после переносим в данные новую таблицу
    # по следующему принципу: группируем по первичному ключу, а остальные колонки суммируем
    drop_table = '''DROP TABLE IF EXISTS templ1 CASCADE;'''
    create_table_query = '''CREATE TABLE templ1
                              (oper_date DATE NOT NULL,
                               credit_account_rk NUMERIC NOT NULL,
                               debet_account_rk NUMERIC NOT NULL,
                               credit_amount FLOAT,
                               debet_amount FLOAT); '''
    sql_query(conn, drop_table)
    sql_query(conn, create_table_query)
    print("Таблица успешно создана в PostgreSQL")

    logging_start(conn,'FT_POSTING_F')
    for index, row in rows_table.posting.iterrows():
        row_posting = (
            row['OPER_DATE'], row['CREDIT_ACCOUNT_RK'], row['DEBET_ACCOUNT_RK'], row['CREDIT_AMOUNT'],
            row['DEBET_AMOUNT'])
        sql_query(conn, "insert into templ values ( %s,%s, %s,%s, %s) ;", row_posting)
    print("Данные во временную таблицу добавлены")
    sql_query(conn,
        "insert into DS.FT_POSTING_F (select oper_date, credit_account_rk, debet_account_rk, SUM(credit_amount), SUM(debet_amount) from templ group by oper_date,debet_account_rk,credit_account_rk) ON CONFLICT DO NOTHING;")
    end_of_logging(conn,'FT_POSTING_F')
    print("Данные в таблицу FT_POSTING_F добавлены")

    # Для избавления от дубляжей, создаем временную таблицу, после переносим в данные новую таблицу
    # по следующему принципу: группируем по первичному ключу, а остальные колонки суммируем
    drop_table = '''DROP TABLE IF EXISTS templ2 CASCADE;'''
    create_table_query = '''CREATE TABLE templ2(
                                    data_actual_date DATE NOT NULL,
                                    data_actual_end_date DATE,
                                    currency_rk NUMERIC NOT NULL,
                                    reduced_cource FLOAT,
                                    code_iso_num VARCHAR(3)); '''
    sql_query(conn, drop_table)
    sql_query(conn, create_table_query)
    print("Таблица успешно создана в PostgreSQL")

    logging_start(conn,'MD_EXCHANGE_RATE_D')
    for index, row in rows_table.exchange_rate.iterrows():
        row_exchange_rate = (
            row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_RK'], row['REDUCED_COURCE'],
            row['CODE_ISO_NUM'])
        sql_query(conn, "insert into templ2 values ( %s, %s, %s, %s, %s) ;", row_exchange_rate)
    print("Данные во временную таблицу добавлены")
    sql_query(conn,
        "insert into DS.MD_EXCHANGE_RATE_D (select data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num from templ2 group by data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)  ON CONFLICT DO NOTHING;")
    end_of_logging(conn,'MD_EXCHANGE_RATE_D')
    print("Данные в таблицу MD_EXCHANGE_RATE_D добавлены")

    logging_start(conn,'MD_LEDGER_ACCOUNT_S')
    for index, row in rows_table.ledger_account.iterrows():
        row_ledger_account = (
            row['CHAPTER'], row['CHAPTER_NAME'], row['SECTION_NUMBER'], row['SECTION_NAME'], row['SUBSECTION_NAME'],
            row['LEDGER1_ACCOUNT'], row['LEDGER1_ACCOUNT_NAME'], row['LEDGER_ACCOUNT'], row['LEDGER_ACCOUNT_NAME'],
            row['CHARACTERISTIC'], row['IS_RESIDENT'], row['IS_RESERVE'], row['IS_RESERVED'], row['IS_LOAN'],
            row['IS_RESERVED_ASSETS'], row['IS_OVERDUE'], row['IS_INTEREST'], row['PAIR_ACCOUNT'], row['START_DATE'],
            row['END_DATE'], row['IS_RUB_ONLY'], row['MIN_TERM'], row['MIN_TERM_MEASURE'], row['MAX_TERM'],
            row['MAX_TERM_MEASURE'], row['LEDGER_ACC_FULL_NAME_TRANSLIT'], row['IS_REVALUATION'], row['IS_CORRECT'],
            row['CHAPTER'], row['CHAPTER_NAME'], row['SECTION_NUMBER'], row['SECTION_NAME'], row['SUBSECTION_NAME'],
            row['LEDGER1_ACCOUNT'], row['LEDGER1_ACCOUNT_NAME'], row['LEDGER_ACCOUNT_NAME'], row['CHARACTERISTIC'],
            row['IS_RESIDENT'], row['IS_RESERVE'], row['IS_RESERVED'], row['IS_LOAN'], row['IS_RESERVED_ASSETS'],
            row['IS_OVERDUE'], row['IS_INTEREST'], row['PAIR_ACCOUNT'], row['END_DATE'], row['IS_RUB_ONLY'],
            row['MIN_TERM'], row['MIN_TERM_MEASURE'], row['MAX_TERM'], row['MAX_TERM_MEASURE'],
            row['LEDGER_ACC_FULL_NAME_TRANSLIT'],
            row['IS_REVALUATION'], row['IS_CORRECT'])
        sql_query(conn,
            "insert into DS.MD_LEDGER_ACCOUNT_S values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (ledger_account,start_date) DO UPDATE SET CHAPTER = %s, "
            "CHAPTER_NAME = %s, SECTION_NUMBER = %s, SECTION_NAME = %s, SUBSECTION_NAME = %s, LEDGER1_ACCOUNT = %s, LEDGER1_ACCOUNT_NAME = %s, "
            "LEDGER_ACCOUNT_NAME = %s, CHARACTERISTIC = %s, IS_RESIDENT = %s, IS_RESERVE = %s, IS_RESERVED = %s, IS_LOAN = %s, IS_RESERVED_ASSETS = %s, "
            "IS_OVERDUE = %s, IS_INTEREST = %s, PAIR_ACCOUNT = %s, END_DATE = %s, IS_RUB_ONLY = %s, MIN_TERM = %s, MIN_TERM_MEASURE = %s, MAX_TERM = %s, "
            "MAX_TERM_MEASURE = %s, LEDGER_ACC_FULL_NAME_TRANSLIT = %s, IS_REVALUATION = %s, IS_CORRECT = %s;",
            row_ledger_account)
    end_of_logging(conn,'MD_LEDGER_ACCOUNT_S')
    print("Данные в таблицу MD_LEDGER_ACCOUNT_S добавлены")


def etl_process_start():
    print("\nНачинается загрузка данных\n")
    conn = psycopg2.connect(host=cdb.host, database=cdb.dbname, user=cdb.user, password=cdb.password)
    sql_query(conn,"insert into LOGS.log_table (log_data, log_time) values ('Начало загрузки данных',  LOCALTIMESTAMP(0));")
    data_loading(conn)
    sql_query(conn, "insert into LOGS.log_table (log_data, log_time) values ('Загрузка данных завершена',  LOCALTIMESTAMP(0));")
    conn.close()
    print("\nЗагрузка данных завершена\n")
