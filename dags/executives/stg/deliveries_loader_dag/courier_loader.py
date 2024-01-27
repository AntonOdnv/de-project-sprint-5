from datetime import datetime
from logging import Logger

from executives.stg import EtlSetting, StgEtlSettingsRepository
from executives.stg.deliveries_loader_dag.pg_saver import PgSaver
from executives.stg.deliveries_loader_dag.api_reader import apiReader
from lib import PgConnect
from lib.dict_util import json2str


class courierLoader:

    WF_KEY = "couriers_origin_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: apiReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2024, 1, 1).isoformat()
                    }
                )
                
            load_queue = self.collection_loader.get_objects()
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0
            
            for courier_row in load_queue:
                self.pg_saver.save_object(conn, 'stg.deliveries_courier', str(courier_row["_id"]), courier_row, datetime.now())

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = datetime.now()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
