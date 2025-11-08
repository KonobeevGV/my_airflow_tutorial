from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Аргументы по умолчанию для всех задач в этом DAG
default_args = {
    'owner': 'grigorii',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Определяем DAG
with DAG(
    dag_id='my_first_dag_v1',  # Уникальное имя DAG
    default_args=default_args,
    description='Очень простой DAG для демонстрации',
    start_date=datetime(2024, 1, 1), # Дата, с которой DAG начнет запускаться по расписанию
    schedule=timedelta(days=1),  # ИЗМЕНЕНИЕ: schedule вместо schedule_interval
    catchup=False, # Не запускать пропущенные запуски (с start_date до сегодня)
    tags=['example', 'tutorial'],
) as dag:

    # Задача 1: Начало пайплайна (просто заглушка)
    start = EmptyOperator(task_id='start')

    # Задача 2: Скачать "данные" (используем BashOperator)
    download_data = BashOperator(
        task_id='download_data',
        bash_command='echo "Скачиваю данные..." && sleep 5' # Имитация работы
    )

    # Задача 3: Обработать данные (используем PythonOperator)
    def process_data_func():
        print("Обрабатываю скачанные данные...")
        # Здесь могла бы быть настоящая логика, например, работа с Pandas
        return "Данные обработаны успешно"

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data_func
    )

    # Задача 4: Отправить "уведомление"
    send_notification = BashOperator(
        task_id='send_notification',
        bash_command='echo "Уведомление: Пайплайн успешно завершен!"'
    )

    # Конец пайплайна (просто заглушка)
    end = EmptyOperator(task_id='end')

    # Определяем порядок выполнения задач
    # start -> download_data -> process_data -> send_notification -> end
    start >> download_data >> process_data >> send_notification >> end