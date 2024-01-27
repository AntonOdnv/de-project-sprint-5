from logging import Logger
from typing import List

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ledgersObj(BaseModel):
    courier_id: int  
    courier_name: str  
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class ledgersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ledgers(self) -> List[ledgersObj]:
        with self._db.client().cursor(row_factory=class_row(ledgersObj)) as cur:
            cur.execute(
                """
                    WITH courier_data AS (
                    WITH cte AS (
                    SELECT c.id AS courier_id, 
                        c.courier_name AS courier_name,
                        EXTRACT(YEAR FROM t.ts) AS settlement_year, 
                        EXTRACT(MONTH FROM t.ts) AS settlement_month, 
                        f.order_id,
                        f.total_sum,
                        d.rate,
                        d.tip_sum
                    FROM dds.fct_product_sales AS f
                    INNER JOIN dds.dm_orders AS o ON f.order_id = o.id
                    INNER JOIN dds.dm_timestamps AS t ON o.timestamp_id = t.id
                    INNER JOIN dds.dm_deliveries AS d ON o.id = d.order_id
                    INNER JOIN dds.dm_couriers AS c ON d.courier_id = c.id
                    WHERE t.ts < (date_trunc('MONTH', CURRENT_DATE)::timestamp))
                    SELECT courier_id, 
                        courier_name, 
                        settlement_year, 
                        settlement_month,
                        count(DISTINCT(order_id)) AS orders_count,
                        sum(total_sum) AS orders_total_sum,
                        avg(rate) AS rate_avg,
                        sum(total_sum)*0.25 AS order_processing_fee,
                        sum(tip_sum) AS courier_tips_sum	   
                    FROM cte
                    GROUP BY courier_id, courier_name, settlement_year, settlement_month)
                    SELECT courier_id, 
                        courier_name, 
                        settlement_year, 
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        case when rate_avg < 4 then greatest(orders_total_sum * 0.05, 100)
                                when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, 150)
                                when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, 175)
                                when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, 200)
                            end courier_order_sum,
                        courier_tips_sum,
                        (case when rate_avg < 4 then greatest(orders_total_sum * 0.05, 100)
                                when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, 150)
                                when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, 175)
                                when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, 200)
                            end + courier_tips_sum) * 0.95 courier_reward_sum 
                    FROM courier_data
                """
            )
            objs = cur.fetchall()
        return objs


class ledgersDestRepository:

    def insert_ledgers(self, conn: Connection, ledgers: ledgersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee,
                                               courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, 
                            %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET 
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": ledgers.courier_id,
                    "courier_name": ledgers.courier_name,
                    "settlement_year": ledgers.settlement_year,
                    "settlement_month": ledgers.settlement_month,
                    "orders_count": ledgers.orders_count,
                    "orders_total_sum": ledgers.orders_total_sum,
                    "rate_avg": ledgers.rate_avg,
                    "order_processing_fee": ledgers.order_processing_fee,
                    "courier_order_sum": ledgers.courier_order_sum,
                    "courier_tips_sum": ledgers.courier_tips_sum,
                    "courier_reward_sum": ledgers.courier_reward_sum
                },
            )


class ledgerLoader:

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ledgersOriginRepository(pg_origin)
        self.dds = ledgersDestRepository()
        self.log = log

    def load_ledgers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Вычитываем очередную пачку объектов.
            load_queue = self.origin.list_ledgers()
            self.log.info(f"Found {len(load_queue)} ledgers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for ledgers in load_queue:
                self.dds.insert_ledgers(conn, ledgers)

            self.log.info(f"Load finished")
