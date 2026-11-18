"""Opt-in real integration test. Never executes in offline unittest discovery.

Usage:
  TEST_BITRIX_URL=<test-portal-webhook> python tests/test_bitrix_integration.py

Creates a synthetic contact and lead in b24-frslge only, verifies them,
then deletes exactly the IDs returned by the two add operations.
"""

import os
import sys
import uuid
from urllib.parse import urlparse



TEST_HOST = 'b24-frslge.bitrix24.ru'
UID_FIELD = 'UF_CRM_1776153852'


def verify_test_url(url):
    parsed = urlparse(url)
    if parsed.scheme != 'https' or parsed.hostname != TEST_HOST:
        raise RuntimeError('Refusing non-test Bitrix24 portal')
    if not parsed.path.startswith('/rest/') or not url.endswith('/'):
        raise RuntimeError('Expected a complete test REST webhook base')


def run(url):
    import requests
    verify_test_url(url)
    created = {'contact': None, 'lead': None}
    marker = 'TG_BOT_INTEGRATION_TEST_' + uuid.uuid4().hex[:12]

    def call(method, params):
        try:
            response = requests.post(url + method, json=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            # Request exceptions may embed the secret URL: don't print them.
            raise RuntimeError(f'Bitrix request failed: {method}: {type(exc).__name__}') from None
        if not isinstance(payload, dict) or payload.get('error') or 'result' not in payload:
            raise RuntimeError(f'Bitrix rejected {method}: no valid result')
        return payload['result']

    # Fail before ANY mutation if the test portal schema is incompatible.
    fields = call('crm.lead.fields', {})
    if UID_FIELD not in fields:
        raise RuntimeError(f'Test portal missing {UID_FIELD}; no test records created')

    try:
        created['contact'] = call('crm.contact.add', {'fields': {
            'NAME': marker,
            'PHONE': [{'VALUE': '+79990000001', 'VALUE_TYPE': 'WORK'}],
            'COMMENTS': marker + ' synthetic integration fixture',
        }})
        if not isinstance(created['contact'], int) or created['contact'] <= 0:
            raise RuntimeError('Invalid contact ID')
        created['lead'] = call('crm.lead.add', {'fields': {
            'TITLE': marker, 'NAME': marker,
            'CONTACT_ID': created['contact'],
            UID_FIELD: marker,
            'COMMENTS': marker + ' synthetic integration fixture',
        }})
        if not isinstance(created['lead'], int) or created['lead'] <= 0:
            raise RuntimeError('Invalid lead ID')
        lead = call('crm.lead.get', {'id': created['lead']})
        assert lead.get(UID_FIELD) == marker, 'UID not persisted'
        assert str(lead.get('CONTACT_ID')) == str(created['contact']), 'Contact not linked'
        matches = call('crm.lead.list', {
            'filter': {UID_FIELD: marker}, 'select': ['ID', UID_FIELD]
        })
        assert len([x for x in matches if x.get(UID_FIELD) == marker]) == 1
        print('PASS: contact, lead, UID lookup and CRM linkage')
    finally:
        # Only IDs created by THIS run, not phone/search matches.
        for entity in ('lead', 'contact'):
            record_id = created[entity]
            if record_id is None:
                continue
            try:
                call(f'crm.{entity}.delete', {'id': record_id})
                print(f'CLEANUP_OK: {entity} ID={record_id}')
            except Exception:
                # Do not hide cleanup failure; exact IDs are required for recovery.
                print(f'CLEANUP_REQUIRED: {entity} ID={record_id}', file=sys.stderr)
                raise


if __name__ == '__main__':
    run(os.environ.get('TEST_BITRIX_URL', ''))
