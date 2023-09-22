import os
from utils.notion_utils import create_database, create_record
from utils.model_utils import models_to_write
import json

MODEL_SELECT_METHOD = os.environ['MODEL_SELECT_METHOD']
MODEL_SELECT_REGEX = os.environ['MODEL_SELECT_REGEX']
MODEL_SELECT_LIST = os.environ['MODEL_SELECT_LIST']
  
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
    print(f'models to sync { sync_models_dict }')
    # Create or update the database
    database_id = create_database()

    current_model_count = 0
    for model_name, data in sorted(sync_models_dict.items(), reverse=True):
        create_record(database_id, model_name, data, catalog_nodes)
        current_model_count = current_model_count + 1
        print(f'{current_model_count} models processed out of { sync_models_len }')

if __name__ == '__main__':
    main()