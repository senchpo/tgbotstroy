import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    'integration', Path(__file__).with_name('test_bitrix_integration.py'))
integration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(integration)


class GuardTests(unittest.TestCase):
    def test_allows_exact_test_portal(self):
        integration.verify_test_url('https://b24-frslge.bitrix24.ru/rest/1/secret/')

    def test_refuses_production_and_lookalike(self):
        urls = [
            'https://b24-94kja2.bitrix24.ru/rest/1/token/',
            'https://b24-frslge.bitrix24.ru.attacker.test/rest/1/token/',
            'http://b24-frslge.bitrix24.ru/rest/1/token/',
            'https://b24-frslge.bitrix24.ru/crm/lead/',
        ]
        for url in urls:
            with self.subTest(url=url), self.assertRaises(RuntimeError):
                integration.verify_test_url(url)
