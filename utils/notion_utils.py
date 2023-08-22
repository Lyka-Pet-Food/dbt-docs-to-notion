from utils.request_utils import make_request, get_paths_or_empty
from utils.model_utils import get_owner
import json
from datetime import datetime
import os

DATABASE_PARENT_ID = os.environ['DATABASE_PARENT_ID']
DATABASE_NAME = os.environ['DATABASE_NAME']

def create_database():
    children_query_resp = make_request(
        endpoint='blocks/',
        querystring=f'{DATABASE_PARENT_ID}/children',
        method='GET'
    )

    database_id = ''
    for child in children_query_resp['results']:
        if 'child_database' in child and child['child_database'] == {'title': DATABASE_NAME}:
            database_id = child['id']
            break

    if database_id:
        print(f'database {database_id} already exists, proceeding to update records!')
    else:
        database_obj = {
            "title": [
                {
                    "type": "text",
                    "text": {
                        "content": DATABASE_NAME,
                        "link": None
                    }
                }
            ],
            "parent": {
                "type": "page_id",
                "page_id": DATABASE_PARENT_ID
            },
            "properties": {
                "Name": {"title": {}},
                "Description": {"rich_text": {}},
                "Owner": {"rich_text": {}},
                "Relation": {"rich_text": {}},
                "Approx Rows": {"number": {"format": "number_with_commas"}},
                "Approx GB": {"number": {"format": "number_with_commas"}},
                "Depends On": {"rich_text": {}},
                "Tags": {"rich_text": {}}
            }
        }

        print('creating database')
        database_creation_resp = make_request(
            endpoint='databases/',
            querystring='',
            method='POST',
            json=database_obj
        )
        database_id = database_creation_resp['id']
        print(f'\ncreated database {database_id}, proceeding to create records!')
    return database_id

def update_record(record_id, record_obj):
    current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    record_obj["properties"]["Docs Last Updated"] = {
        "rich_text": [{"text": {"content": current_datetime}}]
    }

    _record_update_resp = make_request(
        endpoint=f'pages/{record_id}',
        querystring='',
        method='PATCH',
        json=record_obj
    )

    # children can't be updated via record update, so we'll delete and re-add
    record_children_resp = make_request(
        endpoint='blocks/',
        querystring=f'{record_id}/children',
        method='GET'
    )
    for record_child in record_children_resp['results']:
        record_child_id = record_child['id']
        _record_child_deletion_resp = make_request(
            endpoint='blocks/',
            querystring=record_child_id,
            method='DELETE'
        )

    _record_children_replacement_resp = make_request(
        endpoint='blocks/',
        querystring=f'{record_id}/children',
        method='PATCH',
        json={"children": record_obj['children']}
    )

def create_record(database_id, model_name, data, catalog_nodes):
    column_descriptions = {name: metadata['description'] for name, metadata in data['columns'].items()}
    col_names_and_data = list(get_paths_or_empty(catalog_nodes, [[model_name, 'columns']], {}).items())
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    columns_table_children_obj = [
        {
            "type": "table_row",
            "table_row": {
                "cells": [
                    [{"type": "text", "text": {"content": "Column"}, "plain_text": "Column"}],
                    [{"type": "text", "text": {"content": "Type"}, "plain_text": "Type"}],
                    [{"type": "text", "text": {"content": "Description"}, "plain_text": "Description"}]
                ]
            }
        }
    ]

    for (col_name, col_data) in col_names_and_data[:98]:
        column_description = column_descriptions.get(col_name.lower(), '')
        columns_table_children_obj.append(
            {
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": col_name}, "plain_text": col_name}],
                        [{"type": "text", "text": {"content": col_data['type']}, "plain_text": col_data['type']}],
                        [{"type": "text", "text": {"content": column_description}, "plain_text": column_description}]
                    ]
                }
            }
        )

    if len(col_names_and_data) > 98:
        columns_table_children_obj.append(
            {
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": "..."}, "plain_text": "..."}],
                        [{"type": "text", "text": {"content": "..."}, "plain_text": "..."}],
                        [{"type": "text", "text": {"content": "..."}, "plain_text": "..."}]
                    ]
                }
            }
        )

    record_children_obj = [
        # Table of contents
        {
            "object": "block",
            "type": "table_of_contents",
            "table_of_contents": {
                "color": "default"
            }
        },
        # Columns
        {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "Columns"}
                    }
                ]
            }
        },
        {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": 3,
                "has_column_header": True,
                "has_row_header": False,
                "children": columns_table_children_obj
            }
        },
        # Raw SQL
        {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "Raw SQL"}
                    }
                ]
            }
        },
        {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": data['raw_code'][:2000] if 'raw_code' in data else data['raw_sql'][:2000]
                        }
                    }
                ],
                "language": "sql"
            }
        },
        # Compiled SQL
        {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "Compiled SQL"}
                    }
                ]
            }
        },
        {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": data['compiled_code'][:2000] if 'compiled_code' in data else data['compiled_sql'][:2000]
                        }
                    }
                ],
                "language": "sql"
            }
        },
        # Last Updated
        {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "Last Updated"}
                    }
                ]
            }
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": formatted_datetime}
                    }
                ]
            }
        }
    ]

    record_obj = {
        "parent": {
            "database_id": database_id
        },
        "properties": {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": data['name']
                        }
                    }
                ]
            },
            "Description": {
                "rich_text": [
                    {
                        "text": {
                            "content": data['description'][:2000]
                        }
                    }
                ]
            },
            "Owner": {
                "rich_text": [
                    {
                        "text": {
                            "content": str(get_owner(data, catalog_nodes, model_name))[:2000]
                        }
                    }
                ]
            },
            "Relation": {
                "rich_text": [
                    {
                        "text": {
                            "content": data['relation_name'][:2000]
                        }
                    }
                ]
            },
            "Approx Rows": {
                "number": get_paths_or_empty(
                    catalog_nodes,
                    [[model_name, 'stats', 'num_rows', 'value'],
                     [model_name, 'stats', 'row_count', 'value']],
                    -1
                )
            },
            "Approx GB": {
                "number": get_paths_or_empty(
                    catalog_nodes,
                    [[model_name, 'stats', 'bytes', 'value'],
                     [model_name, 'stats', 'num_bytes', 'value']],
                    0
                ) / 1e9
            },
            "Depends On": {
                "rich_text": [
                    {
                        "text": {
                            "content": json.dumps(data['depends_on'])[:2000]
                        }
                    }
                ]
            },
            "Tags": {
                "rich_text": [
                    {
                        "text": {
                            "content": json.dumps(data['tags'])[:2000]
                        }
                    }
                ]
            }
        },
        "children": record_children_obj
    }

    query_obj = {
        "filter": {
            "property": "Name",
            "title": {
                "equals": data['name']
            }
        }
    }

    try:
        record_query_resp = make_request(
            endpoint=f'databases/{database_id}/query',
            querystring='',
            method='POST',
            json=query_obj
        )
    except json.JSONDecodeError as e:
        print(f'Skipping {model_name} due to JSON decode error:', e)
        return  # Skip this model_name and proceed to the next one


    if record_query_resp['results']:
        print(f'updating {model_name} record')
        record_id = record_query_resp['results'][0]['id']
        update_record(record_id, record_obj)

    else:
        print(f'creating {model_name} record')
        _record_creation_resp = make_request(
            endpoint='pages/',
            querystring='',
            method='POST',
            json=record_obj
        )
