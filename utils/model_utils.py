from utils.request_utils import get_paths_or_empty
import re

def get_owner(data, catalog_nodes, model_name):
  """
  Check for an owner field explicitly named in the DBT Config
  If none present, fall back to database table owner
  """
  owner = get_paths_or_empty(data, [['config', 'meta', 'owner']], None)
  if owner is not None:
    return owner

  return get_paths_or_empty(catalog_nodes, [[model_name, 'metadata', 'owner']], '')

def models_to_write(model_select_method, all_models_dict, model_select_list = [''], MODEL_SELECT_REGEX = ''):
    if model_select_method == 'select':
        sync_models_dict = model_select_list

    elif model_select_method == 'regex':
        print(f'{MODEL_SELECT_REGEX} string used for select')
        model_name_pattern = re.compile(MODEL_SELECT_REGEX)
        sync_models_dict = {
            node_name: data
            for (node_name, data) in all_models_dict.items()
            if model_name_pattern.match(data['name'])
        }
    else:
        sync_models_dict = all_models_dict
        
    sync_models_len = len(sync_models_dict)
    print(f'{ sync_models_len } selected for sync')
    return sync_models_dict, sync_models_len