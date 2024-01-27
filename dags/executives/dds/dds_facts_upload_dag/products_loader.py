from logging import Logger
from typing import List
from datetime import datetime

from executives.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class productObj(BaseModel):
    id: int
    restaurant_id: str
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime


class productsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[productObj]:
        with self._db.client().cursor(row_factory=class_row(productObj)) as cur:
            cur.execute(
                """
                    WITH cte AS (
                           SELECT id, 
                           object_value::json->>'_id' AS restaurant_id,
                           json_array_elements((object_value::json->>'menu')::json) AS menu,
                           (object_value::json->>'update_ts')::timestamp AS active_from,
                           '2099-12-31 00:00:00.000'::timestamp AS active_to
                    FROM stg.ordersystem_restaurants
                    )
                    SELECT DISTINCT id::int, 
                    	   id::int AS restaurant_id,
                    	   menu::json->> '_id' AS product_id,
                    	   menu::json->> 'name' AS product_name,
                    	   menu::json->> 'price' AS product_price,
                    	   active_from,
                    	   active_to
                    FROM cte
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.                    
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class productDestRepository:

    def insert_product(self, conn: Connection, product: productObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "id": product.id,
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )


class productLoader:
    WF_KEY = "products_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = productsOriginRepository(pg_origin)
        self.dds = productDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.dds.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
