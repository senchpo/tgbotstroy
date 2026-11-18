"""Read-only Bitrix24 schema audit. Prints no webhook or customer records.

Usage: BITRIX_URL=<existing-webhook-base> python audit_crm_fields.py
The env value must end in '/' and is never printed.
"""

import json
import os
import sys

import requests


METHODS = (
    ('lead_fields', 'crm.lead.fields', {}),
    ('contact_fields', 'crm.contact.fields', {}),
    ('lead_userfields', 'crm.lead.userfield.list', {}),
    ('sources', 'crm.status.list', {'filter': {'ENTITY_ID': 'SOURCE'}}),
    ('lead_types', 'crm.status.list', {'filter': {'ENTITY_ID': 'TYPE'}}),
    ('lead_statuses', 'crm.status.list', {'filter': {'ENTITY_ID': 'STATUS'}}),
)


def read_schema(base_url: str, post=requests.post) -> dict:
    if not base_url.startswith('https://') or not base_url.endswith('/'):
        raise ValueError('BITRIX_URL must be an HTTPS webhook base ending in /')
    output = {}
    for label, method, payload in METHODS:
        response = post(base_url + method, json=payload, timeout=20)
        response.raise_for_status()
        parsed = response.json()
        if not isinstance(parsed, dict) or 'error' in parsed or 'result' not in parsed:
            raise RuntimeError(f'{method} returned no valid result')
        data = parsed['result']
        if label.endswith('_fields') and isinstance(data, dict):
            output[label] = [{
                'code': code, 'title': details.get('title', ''),
                'type': details.get('type', ''),
                'required': bool(details.get('isRequired')),
            } for code, details in data.items() if isinstance(details, dict)]
        elif label == 'lead_userfields' and isinstance(data, list):
            output[label] = [{
                'field_name': row.get('FIELD_NAME'),
                'user_type_id': row.get('USER_TYPE_ID'),
                'multiple': row.get('MULTIPLE'),
            } for row in data if isinstance(row, dict)]
        elif isinstance(data, list):
            output[label] = [{
                'id': row.get('STATUS_ID'), 'name': row.get('NAME'),
                'entity_id': row.get('ENTITY_ID'),
            } for row in data if isinstance(row, dict)]
        else:
            raise RuntimeError(f'{method} returned unsupported schema shape')
    return output


if __name__ == '__main__':
    url = os.environ.get('BITRIX_URL', '')
    try:
        result = read_schema(url)
    except Exception as exc:
        # No raw network exception: HTTP exceptions can include webhook URLs.
        print(f'CRM metadata audit failed ({type(exc).__name__})', file=sys.stderr)
        sys.exit(1)
    print(json.dumps(result, ensure_ascii=False, indent=2))
