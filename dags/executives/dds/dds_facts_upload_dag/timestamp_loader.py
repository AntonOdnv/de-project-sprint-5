from logging import Logger
from typing import List
from datetime import datetime, date, time

from executives.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class timestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class timestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamp(self, timestamp_threshold: int, limit: int) -> List[timestampObj]:
        with self._db.client().cursor(row_factory=class_row(timestampObj)) as cur:
            cur.execute(
                """
                    WITH cte AS (
                    SELECT id,
                        CASE  
                            WHEN object_value::json->>'final_status' = 'CLOSED' OR object_value::json->>'final_status' = 'CANCELLED'
                            THEN (object_value::json->>'date')::timestamp
                        END AS ts
                    FROM stg.ORDERSYSTEM_ORDERS)
                    SELECT *, 
                        extract('year' FROM ts) AS "year",
                        extract('month' FROM ts) AS "month",
                        extract('day' FROM ts) AS "day",
                        ts::date AS date,
                        ts::time AS time
                    FROM cte
                    WHERE ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY ts DESC 
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class timestampDestRepository:

    def insert_timestamp(self, conn: Connection, timestamp: timestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, date, time)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                """,
                {
                    "id": timestamp.id,
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time,
                },
            )


class timestampLoader:
    WF_KEY = "timestamp_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = timestampOriginRepository(pg_origin)
        self.dds = timestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamp(self):
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
            load_queue = self.origin.list_timestamp(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamp to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestamp in load_queue:
                self.dds.insert_timestamp(conn, timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
