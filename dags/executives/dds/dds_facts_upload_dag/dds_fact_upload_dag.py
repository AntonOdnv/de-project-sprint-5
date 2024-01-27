import logging

import pendulum
from airflow.decorators import dag, task
from executives.dds.dds_facts_upload_dag.facts_loader import factLoader
from executives.dds.dds_facts_upload_dag.orders_loader import orderLoader
from executives.dds.dds_facts_upload_dag.products_loader import productLoader
from executives.dds.dds_facts_upload_dag.restaurants_loader import restaurantLoader
from executives.dds.dds_facts_upload_dag.timestamp_loader import timestampLoader
from executives.dds.dds_facts_upload_dag.users_loader import userLoader
from executives.dds.dds_facts_upload_dag.couriers_loader import courierLoader
from executives.dds.dds_facts_upload_dag.delivery_loader import deliveryLoader
from executives.dds.dds_facts_upload_dag.courier_ledger_loader import ledgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_to_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="facts_load")
    def load_facts():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = factLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_facts()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = orderLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = productLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = restaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="timestamp_load")
    def load_timestamp():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = timestampLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamp()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = userLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        deliveries_loader = deliveryLoader(origin_pg_connect, dwh_pg_connect, log)
        deliveries_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = courierLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.    

    @task(task_id="ledgers_load")
    def load_ledgers():
        # создаем экземпляр класса, в котором реализована логика.
        ledger_loader = ledgerLoader(origin_pg_connect, dwh_pg_connect, log)
        ledger_loader.load_ledgers()  # Вызываем функцию, которая перельет данные. 

    # Инициализируем объявленные таски.
    facts_dict = load_facts()
    orders_dict = load_orders()
    products_dict = load_products()
    restaurants_dict = load_restaurants()
    timestamp_dict = load_timestamp()
    users_dict = load_users()
    couriers_dict = load_couriers()
    delivery_dict = load_deliveries()
    ledger_dict = load_ledgers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [users_dict, restaurants_dict, timestamp_dict] >> orders_dict >> products_dict >> facts_dict  
    couriers_dict >> orders_dict >> delivery_dict
    [facts_dict, delivery_dict] >> ledger_dict

stg_to_dds_dag = sprint5_stg_to_dds_dag()