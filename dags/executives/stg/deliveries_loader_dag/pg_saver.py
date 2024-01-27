from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, db_name: str, id: str, val: Any, update_ts: datetime):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {db_name}(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "db_name": db_name,
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )
