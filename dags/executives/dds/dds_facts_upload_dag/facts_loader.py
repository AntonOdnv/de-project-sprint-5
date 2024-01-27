from logging import Logger
from typing import List
from datetime import datetime

from executives.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class factObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class factsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_facts(self, fact_threshold: int, limit: int) -> List[factObj]:
        with self._db.client().cursor(row_factory=class_row(factObj)) as cur:
            cur.execute(
                """
                    WITH cte AS (
					SELECT event_value::json->>'order_id' AS oid,
						   (json_array_elements((event_value::json->>'product_payments')::json))::json->>'product_id' AS pid,
						   json_array_elements((event_value::json->>'product_payments')::json) AS events
					FROM stg.BONUSSYSTEM_EVENTS
					WHERE event_type = 'bonus_transaction')
					SELECT product_id_dest AS product_id, 
                           order_id,						   
						   (events::json->>'quantity')::int AS count,
						   (events::json->>'price')::numeric(19,5) AS price,
						   (events::json->>'product_cost')::numeric(19,5) AS total_sum,
						   (events::json->>'bonus_payment')::numeric(19,5) AS bonus_payment,
						   (events::json->>'bonus_grant')::numeric(19,5) AS bonus_grant
					FROM cte
					INNER JOIN (SELECT id AS order_id, order_key FROM dds.dm_orders WHERE order_status = 'CLOSED') AS ord ON ord.order_key = cte.oid
                    INNER JOIN (SELECT id AS product_id_dest, product_id FROM dds.dm_products) AS p ON cte.pid = p.product_id
                """, {
                    "threshold": fact_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class factDestRepository:

    def insert_fact(self, conn: Connection, fact: factObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (product_id, order_id) DO UPDATE
                    SET
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": fact.product_id,
                    "order_id": fact.order_id,
                    "count": fact.count,
                    "price": fact.price,
                    "total_sum": fact.total_sum,
                    "bonus_payment": fact.bonus_payment,
                    "bonus_grant": fact.bonus_grant
                },
            )


class factLoader:
    WF_KEY = "facts_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = factsOriginRepository(pg_origin)
        self.dds = factDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_facts(self):
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
            load_queue = self.origin.list_facts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} facts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fact in load_queue:
                self.dds.insert_fact(conn, fact)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
