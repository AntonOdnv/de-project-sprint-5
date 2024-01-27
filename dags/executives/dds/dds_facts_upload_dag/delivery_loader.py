from logging import Logger
from typing import List
from datetime import datetime, date, time

from executives.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class deliveriesObj(BaseModel):
    id: int    
    delivery_id: str
    order_id: int
    courier_id: int
    rate: int
    tip_sum: float
    order_ts: datetime


class deliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, deliveries_threshold: int, limit: int) -> List[deliveriesObj]:
        with self._db.client().cursor(row_factory=class_row(deliveriesObj)) as cur:
            cur.execute(
                """
                    WITH cte AS (
                            SELECT id,
                                object_value::json->>'order_id'    AS order_id,
                                object_value::json->>'order_ts'    AS order_ts,
                                object_value::json->>'delivery_id' AS delivery_id,
                                object_value::json->>'courier_id'  AS courier_id,
                                object_value::json->>'rate'	      AS rate,
                                object_value::json->>'tip_sum'     AS tip_sum
                            FROM stg.deliveries_delivery),
                        order_link AS (
                            SELECT id,
                                object_value::json->>'_id' AS order_key
                            FROM stg.ordersystem_orders)
                    SELECT cte.id, 
                        cte.delivery_id,
                        ol.id AS order_id,
                        c.id AS courier_id, 	    
                        cte.rate, 
                        cte.tip_sum,
                        cte.order_ts::timestamp
                    FROM cte
                    INNER JOIN (SELECT id, object_id FROM stg.deliveries_courier) AS c ON cte.courier_id = c.object_id
                    INNER JOIN (SELECT * FROM order_link) AS ol ON cte.order_id = ol.order_key
                    WHERE order_ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY order_ts DESC 
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": deliveries_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class deliveriesDestRepository:

    def insert_deliveries(self, conn: Connection, deliveries: deliveriesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(id, delivery_id, order_id, courier_id, rate, tip_sum, order_ts)
                    VALUES (%(id)s, %(delivery_id)s, %(order_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s, %(order_ts)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        order_ts = EXCLUDED.order_ts;
                """,
                {
                    "id": deliveries.id,
                    "delivery_id": deliveries.delivery_id,
                    "order_id": deliveries.order_id,
                    "courier_id": deliveries.courier_id,
                    "rate": deliveries.rate,
                    "tip_sum": deliveries.tip_sum,
                    "order_ts": deliveries.order_ts,
                },
            )


class deliveryLoader:
    WF_KEY = "deliveries_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = deliveriesOriginRepository(pg_origin)
        self.dds = deliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: '01-01-1970'})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for deliveries in load_queue:
                self.dds.insert_deliveries(conn, deliveries)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
