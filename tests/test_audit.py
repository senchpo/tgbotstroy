import importlib
import sys
import types
import unittest
from unittest.mock import MagicMock


if 'requests' not in sys.modules:
    requests_fake = types.ModuleType('requests')
    requests_fake.post = MagicMock()
    sys.modules['requests'] = requests_fake

audit = importlib.import_module('audit_crm_fields')


class SchemaAuditTests(unittest.TestCase):
    def test_read_only_method_allowlist(self):
        expected = {
            'crm.lead.fields', 'crm.contact.fields',
            'crm.lead.userfield.list', 'crm.status.list',
        }
        self.assertEqual({method for _, method, _ in audit.METHODS}, expected)
        self.assertFalse(any(x.endswith('.add') or x.endswith('.update')
                             for _, x, _ in audit.METHODS))

    def test_schema_output_has_no_secret_url(self):
        def fake_post(url, json, timeout):
            result = {'FIELD': {'title': 'Field', 'type': 'string'}} \
                if url.endswith('.fields') else []
            response = MagicMock()
            response.json.return_value = {'result': result}
            return response

        schema = audit.read_schema('https://portal.example/rest/1/secret/', fake_post)
        self.assertIn('lead_fields', schema)
        self.assertNotIn('secret', repr(schema))


if __name__ == '__main__':
    unittest.main()
