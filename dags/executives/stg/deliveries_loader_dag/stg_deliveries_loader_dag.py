import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from executives.stg.deliveries_loader_dag.pg_saver import PgSaver
from executives.stg.deliveries_loader_dag.courier_loader import courierLoader
from executives.stg.deliveries_loader_dag.delivery_loader import deliveryLoader
from executives.stg.deliveries_loader_dag.api_reader import apiReader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)


def sprint5_stg_couriers_api_loader():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    courier_api_endpoint = Variable.get("courier_api_endpoint")
    delivery_api_endpoint = Variable.get("delivery_api_endpoint")
    headers  = {
        "X-API-KEY": Variable.get("courier_api_X-API-KEY"),
        "X-Nickname": Variable.get("courier_api_X-Nickname"),
        "X-Cohort": Variable.get("courier_api_X-Cohort")
    }

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = apiReader(courier_api_endpoint, headers)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = courierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()    

    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = apiReader(delivery_api_endpoint, headers)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = deliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    courier_loader = load_couriers()
    delivery_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    [courier_loader, delivery_loader]  # type: ignore

courier_api_to_stg_dag = sprint5_stg_couriers_api_loader()  # noqa
