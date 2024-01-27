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
