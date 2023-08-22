import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.notion_utils import create_database, create_record
import json
import time

MODEL_SELECT_METHOD = os.environ['MODEL_SELECT_METHOD']
MODEL_SELECT_REGEX = os.environ['MODEL_SELECT_REGEX']
MODEL_SELECT_LIST = os.environ['MODEL_SELECT_LIST']

def retry(exception, tries=10, delay=0.5, backoff=2):
    def decorator_retry(func):
        def wrapper(*args, **kwargs):
            retry_strategy = Retry(
                total=tries,
                backoff_factor=backoff,
                status_forcelist=[503],
                method_whitelist=["GET"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = requests.Session()
            session.mount("https://", adapter)

            for attempt in range(tries):
                try:
                    response = func(*args, **kwargs)
                    response.raise_for_status()
                    return response.json()
                except exception as e:
                    if attempt == tries - 1:
                        raise e
                    print(f"Retrying after {delay} seconds...")
                    time.sleep(delay)

        return wrapper

    return decorator_retry
  
def main():

    print(f'Sync started, in { MODEL_SELECT_METHOD } select mode')
    # Load nodes from dbt docs
    with open('target/manifest.json', encoding='utf-8') as f:
        manifest = json.load(f)
        manifest_nodes = manifest['nodes']

    with open('target/catalog.json', encoding='utf-8') as f:
        catalog = json.load(f)
        catalog_nodes = catalog['nodes']

    all_models_dict = {
        node_name: data
        for (node_name, data)
        in manifest_nodes.items() if data['resource_type'] == 'model'
    }

    all_models_len = len(all_models_dict)
    print(f'{ all_models_len } models in dbt project')

    sync_models_dict, sync_models_len = models_to_write(MODEL_SELECT_METHOD, all_models_dict, MODEL_SELECT_LIST, MODEL_SELECT_REGEX)

    # Create or update the database
    database_id = create_database()

    current_model_count = 0
    for model_name, data in sorted(sync_models_dict.items(), reverse=True):
        create_record(database_id, model_name, data, catalog_nodes)
        current_model_count = current_model_count + 1
        print(f'{current_model_count} models processed out of { sync_models_len }')

if __name__ == '__main__':
    main()