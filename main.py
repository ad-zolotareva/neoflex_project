import pandas as pd
import psycopg2
import time

conn = psycopg2.connect(
    host="localhost",
    database="DBBank",
    user="Neoflex",
    password="12345",
)


balance_file_name = "/home/alina/Загрузки/задача_1.1/ft_balance_f.csv"
posting_file_name = "/home/alina/Загрузки/задача_1.1/ft_posting_f.csv"
account_file_name = "/home/alina/Загрузки/задача_1.1/md_account_d.csv"
currency_file_name = "/home/alina/Загрузки/задача_1.1/md_currency_d.csv"
exchange_rate_file_name = "/home/alina/Загрузки/задача_1.1/md_exchange_rate_d.csv"
ledger_account_file_name = "/home/alina/Загрузки/задача_1.1/md_ledger_account_s.csv"

balance = pd.read_csv(balance_file_name, keep_default_na=False, delimiter=';')
balance.columns = ['id', 'ON_DATE', 'ACCOUNT_RK', 'CURRENCY_RK', 'BALANCE_OUT']

posting = pd.read_csv(posting_file_name, keep_default_na=False, delimiter=';')
posting.columns = ['id', 'OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK', 'CREDIT_AMOUNT', 'DEBET_AMOUNT']

account = pd.read_csv(account_file_name, keep_default_na=False, delimiter=';')
account.columns = ['id', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'ACCOUNT_RK', 'ACCOUNT_NUMBER', 'CHAR_TYPE',
                   'CURRENCY_RK', 'CURRENCY_CODE']

currency = pd.read_csv(currency_file_name, delimiter=';', keep_default_na=False, encoding='cp866')
currency.columns = ['id', 'CURRENCY_RK', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'CURRENCY_CODE', 'CODE_ISO_CHAR']

exchange_rate = pd.read_csv(exchange_rate_file_name, keep_default_na=False, delimiter=';')
exchange_rate.columns = ['id', 'DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE', 'CURRENCY_RK', 'REDUCED_COURCE',
                         'CODE_ISO_NUM']

ledger_account = pd.read_csv(ledger_account_file_name, keep_default_na=False, delimiter=';', encoding='cp866')
ledger_account.columns = ['id', 'CHAPTER', 'CHAPTER_NAME', 'SECTION_NUMBER', 'SECTION_NAME', 'SUBSECTION_NAME',
                          'LEDGER1_ACCOUNT', 'LEDGER1_ACCOUNT_NAME', 'LEDGER_ACCOUNT', 'LEDGER_ACCOUNT_NAME',
                          'CHARACTERISTIC', 'IS_RESIDENT', 'IS_RESERVE', 'IS_RESERVED', 'IS_LOAN', 'IS_RESERVED_ASSETS',
                          'IS_OVERDUE', 'IS_INTEREST', 'PAIR_ACCOUNT', 'START_DATE', 'END_DATE', 'IS_RUB_ONLY',
                          'MIN_TERM', 'MIN_TERM_MEASURE', 'MAX_TERM', 'MAX_TERM_MEASURE',
                          'LEDGER_ACC_FULL_NAME_TRANSLIT', 'IS_REVALUATION', 'IS_CORRECT']

cursor = conn.cursor()
cursor.execute("insert into LOGS.log_table (log_data, log_time) values ('Начало загрузки данных',  LOCALTIMESTAMP(0));")
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу FT_BALANCE_F',  LOCALTIMESTAMP(0));")
for index, row in balance.iterrows():
    row_balance = (
    row['ON_DATE'], row['ACCOUNT_RK'], row['CURRENCY_RK'], row['BALANCE_OUT'], row['CURRENCY_RK'], row['BALANCE_OUT'])
    cursor.execute(
        "insert into DS.FT_BALANCE_F values ( %s,%s, %s,%s) ON CONFLICT (on_date,account_rk) DO UPDATE SET CURRENCY_RK = %s, BALANCE_OUT = %s;",
        row_balance)
conn.commit()

time.sleep(5)
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу FT_BALANCE_F',  LOCALTIMESTAMP(0));")
conn.commit()

cursor.close()
print("Данные в таблицу FT_BALANCE_F  добавлены", )

cursor = conn.cursor()
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу MD_ACCOUNT_D',  LOCALTIMESTAMP(0));")
for index, row in account.iterrows():
    row_account = (
    row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['ACCOUNT_RK'], row['ACCOUNT_NUMBER'], row['CHAR_TYPE'],
    row['CURRENCY_RK'], row['CURRENCY_CODE'], row['DATA_ACTUAL_END_DATE'], row['ACCOUNT_NUMBER'], row['CHAR_TYPE'],
    row['CURRENCY_RK'], row['CURRENCY_CODE'])
    cursor.execute(
        "insert into DS.MD_ACCOUNT_D values ( %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (data_actual_date,account_rk) DO UPDATE SET DATA_ACTUAL_END_DATE = %s,ACCOUNT_NUMBER = %s, CHAR_TYPE = %s, CURRENCY_RK = %s, CURRENCY_CODE = %s;",
        row_account)
conn.commit()

time.sleep(5)
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу MD_ACCOUNT_D',  LOCALTIMESTAMP(0));")
conn.commit()
cursor.close()
print("Данные в таблицу MD_ACCOUNT_D добавлены")

cursor = conn.cursor()
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу MD_CURRENCY_D',  LOCALTIMESTAMP(0));")
for index, row in currency.iterrows():
    row_currency = (row['CURRENCY_RK'], row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_CODE'],
                    row['CODE_ISO_CHAR'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_CODE'], row['CODE_ISO_CHAR'])
    cursor.execute(
        "insert into DS.MD_CURRENCY_D values ( %s, %s, %s, %s, %s) ON CONFLICT (currency_rk,data_actual_date) DO UPDATE SET DATA_ACTUAL_END_DATE = %s, CURRENCY_CODE = %s, CODE_ISO_CHAR = %s;",
        row_currency)
conn.commit()

time.sleep(5)
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу MD_CURRENCY_D',  LOCALTIMESTAMP(0));")
conn.commit()
cursor.close()
print("Данные в таблицу MD_CURRENCY_D добавлены")

# Для избавления от дубляжей, создаем временную таблицу, после переносим в данные новую таблицу
# по следующему принципу: группируем по первичному ключу, а остальные колонки суммируем
cursor = conn.cursor()
drop_table = '''DROP TABLE IF EXISTS templ1 CASCADE;'''
create_table_query = '''CREATE TABLE templ1
                      (oper_date DATE NOT NULL,
                       credit_account_rk NUMERIC NOT NULL,
                       debet_account_rk NUMERIC NOT NULL,
                       credit_amount FLOAT,
                       debet_amount FLOAT); '''

cursor.execute(drop_table)
cursor.execute(create_table_query)
conn.commit()
cursor.close()
print("Таблица успешно создана в PostgreSQL")

cursor = conn.cursor()
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу FT_POSTING_F',  LOCALTIMESTAMP(0));")
for index, row in posting.iterrows():
    row_posting = (
    row['OPER_DATE'], row['CREDIT_ACCOUNT_RK'], row['DEBET_ACCOUNT_RK'], row['CREDIT_AMOUNT'], row['DEBET_AMOUNT'])
    cursor.execute("insert into templ values ( %s,%s, %s,%s, %s) ;", row_posting)
conn.commit()

cursor.close()
print("Данные во временную таблицу добавлены")

cursor = conn.cursor()
cursor.execute(
    "insert into DS.FT_POSTING_F (select oper_date, credit_account_rk, debet_account_rk, SUM(credit_amount), SUM(debet_amount) from templ group by oper_date,debet_account_rk,credit_account_rk) ON CONFLICT DO NOTHING;")
conn.commit()

time.sleep(5)
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу FT_POSTING_F',  LOCALTIMESTAMP(0));")
conn.commit()
cursor.close()
print("Данные в таблицу FT_POSTING_F добавлены")

# Для избавления от дубляжей, создаем временную таблицу, после переносим в данные новую таблицу
# по следующему принципу: группируем по первичному ключу, а остальные колонки суммируем
cursor = conn.cursor()
drop_table = '''DROP TABLE IF EXISTS templ2 CASCADE;'''
create_table_query = '''CREATE TABLE templ2(
                            data_actual_date DATE NOT NULL,
                            data_actual_end_date DATE,
                            currency_rk NUMERIC NOT NULL,
                            reduced_cource FLOAT,
                            code_iso_num VARCHAR(3)); '''
cursor.execute(drop_table)
cursor.execute(create_table_query)
conn.commit()
cursor.close()
print("Таблица успешно создана в PostgreSQL")

cursor = conn.cursor()
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу MD_EXCHANGE_RATE_D',  LOCALTIMESTAMP(0));")
for index, row in exchange_rate.iterrows():
    row_exchange_rate = (
    row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_RK'], row['REDUCED_COURCE'],
    row['CODE_ISO_NUM'])
    cursor.execute("insert into templ2 values ( %s, %s, %s, %s, %s) ;", row_exchange_rate)

conn.commit()
cursor.close()
print("Данные во временную таблицу добавлены")

cursor = conn.cursor()
cursor.execute(
    "insert into DS.MD_EXCHANGE_RATE_D (select data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num from templ2 group by data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)  ON CONFLICT DO NOTHING;")
conn.commit()

time.sleep(5)
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу MD_EXCHANGE_RATE_D',  LOCALTIMESTAMP(0));")
conn.commit()
cursor.close()
print("Данные в таблицу MD_EXCHANGE_RATE_D добавлены")

cursor = conn.cursor()
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Начало вставки в таблицу MD_LEDGER_ACCOUNT_S',  LOCALTIMESTAMP(0));")
for index, row in ledger_account.iterrows():
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
    row['IS_OVERDUE'], row['IS_INTEREST'], row['PAIR_ACCOUNT'], row['END_DATE'], row['IS_RUB_ONLY'], row['MIN_TERM'],
    row['MIN_TERM_MEASURE'], row['MAX_TERM'], row['MAX_TERM_MEASURE'], row['LEDGER_ACC_FULL_NAME_TRANSLIT'],
    row['IS_REVALUATION'], row['IS_CORRECT'])
    cursor.execute(
        "insert into DS.MD_LEDGER_ACCOUNT_S values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (ledger_account,start_date) DO UPDATE SET CHAPTER = %s, CHAPTER_NAME = %s, SECTION_NUMBER = %s, SECTION_NAME = %s, SUBSECTION_NAME = %s, LEDGER1_ACCOUNT = %s, LEDGER1_ACCOUNT_NAME = %s, LEDGER_ACCOUNT_NAME = %s, CHARACTERISTIC = %s, IS_RESIDENT = %s, IS_RESERVE = %s, IS_RESERVED = %s, IS_LOAN = %s, IS_RESERVED_ASSETS = %s, IS_OVERDUE = %s, IS_INTEREST = %s, PAIR_ACCOUNT = %s, END_DATE = %s, IS_RUB_ONLY = %s, MIN_TERM = %s, MIN_TERM_MEASURE = %s, MAX_TERM = %s, MAX_TERM_MEASURE = %s, LEDGER_ACC_FULL_NAME_TRANSLIT = %s, IS_REVALUATION = %s, IS_CORRECT = %s;",
        row_ledger_account)
conn.commit()

time.sleep(5)
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Окончена вставка в таблицу MD_LEDGER_ACCOUNT_S',  LOCALTIMESTAMP(0));")
cursor.execute(
    "insert into LOGS.log_table (log_data, log_time) values ('Загрузка данных завершена',  LOCALTIMESTAMP(0));")
conn.commit()
cursor.close()
print("Данные в таблицу MD_LEDGER_ACCOUNT_S добавлены")
conn.close()
