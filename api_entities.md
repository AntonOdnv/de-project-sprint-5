Состав витрины:
-------------------------------------------

id — идентификатор записи.
courier_id — ID курьера, которому перечисляем.
courier_name — Ф. И. О. курьера.
settlement_year — год отчёта.
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
orders_count — количество заказов за период (месяц).
orders_total_sum — общая стоимость заказов.
rate_avg — средний рейтинг курьера по оценкам пользователей.
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
r < 4 — 5% от заказа, но не менее 100 р.;
4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
4.9 <= r — 10% от заказа, но не менее 200 р.

Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки. Иногда заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. Это важно, потому что такие случаи могут выпадать в том числе и на последний день месяца. Тогда начисление курьеру относите к дате заказа, а не доставки.

-------------------------------------------

Новые таблицы слоя DDS:
-------------------------------------------

CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id int4 PRIMARY KEY,
    courier_id varchar NOT NULL UNIQUE,
    courier_name varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id int4 PRIMARY KEY,
	delivery_id varchar NOT NULL,
	order_id int4 NOT NULL REFERENCES dds.dm_orders (id),	
	courier_id int4 NOT NULL REFERENCES dds.dm_couriers (id),	
	rate int4 NOT NULL CHECK (rate > 0),
	tip_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (rate >= 0),
	order_ts timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id serial PRIMARY KEY,
    courier_id int4 REFERENCES dds.dm_couriers(id),
    courier_name varchar NOT NULL,
    settlement_year int4 NOT NULL CHECK (settlement_year BETWEEN 2020 AND 2099),
    settlement_month int4 NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count int4 NOT NULL CHECK (orders_count >= 0),
    orders_total_sum numeric(19, 5) NOT NULL CHECK (orders_total_sum >= (0)::numeric),
    rate_avg numeric(2, 1) NOT NULL CHECK (rate_avg > (0)::numeric),
    order_processing_fee numeric(19, 5) NOT NULL CHECK (order_processing_fee > (0)::numeric),
    courier_order_sum numeric(19, 5) NOT NULL CHECK (courier_order_sum > (0)::numeric),
    courier_tips_sum numeric(19, 5) NOT NULL CHECK (courier_tips_sum > (0)::numeric),
    courier_reward_sum numeric(19, 5) NOT NULL CHECK (courier_reward_sum > (0)::numeric),
    CONSTRAINT dm_courier_ledger_uniques UNIQUE (courier_id, settlement_year, settlement_month)
);

-------------------------------------------

Код класса загрузчика данных по api:
-------------------------------------------

from typing import Dict, List
import requests
import json


class apiReader:
    def __init__(self, api_endpoint, headers) -> None:
        self.api_endpoint = api_endpoint
        self.headers = headers

    def get_objects(self, load_threshold='') -> List[Dict]:

        params = {
            'from': load_threshold,
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result = []
        # Получаем актуальный список данных по api
        responce = requests.get(self.api_endpoint, headers=self.headers, params = params)
        while responce.text != '[]':
            list = json.loads(responce.content)
            for list_row in list:
                result.append(list_row)

            params['offset'] += len(list)

            responce = requests.get(self.api_endpoint, headers=self.headers, params = params)

        return result

-------------------------------------------
Значения параметров api_endpoint и headers хранятся в переменных Airflow. 