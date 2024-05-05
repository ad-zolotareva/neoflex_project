from task_1_1.etl_process import etl_process_start
from task_1_2.showcase_calculation import showcase_calculation
from task_1_3.import_data import import_date_start
from task_1_4.wiring_calculation import wiring_calculation_start

if __name__ == '__main__':

    is_continue = True
    while is_continue:
        number = input("1 - Задание 1.1: Запустить ETL-процесс для загрузки банковских данных из csv-файлов в соответствующие таблицы СУБД PostgreSQL\n"
                       "2 - Задание 1.2: Рассчитать витрины данных в слое «DM»: витрину оборотов и витрину 101-й отчётной формы\n"
                       "3 - Задание 1.3: Выгрузить данные с таблицы оборотов в csv-файл или загрузить эти данные в базу данных в дополнительную таблицу\n"
                       "4 - Задание 1.4: Вызвать функцию для расчета проводки\n"
                       "Прочее - ВЫХОД\n"
                       "\nВведите номер действия: "
                       )

        match number:
            case "1":
                etl_process_start()

            case "2":
                showcase_calculation()

            case "3":
                import_date_start()

            case "4":
                wiring_calculation_start()

            case _:
                is_continue = False
    print("\nРабота программы завершена")
