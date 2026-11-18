import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# Test parsing and routing without a live Telegram token or network calls.
fake_telebot = types.ModuleType('telebot')
fake_bot = MagicMock()
fake_bot.message_handler.side_effect = lambda **kwargs: (lambda function: function)
fake_telebot.TeleBot = MagicMock(return_value=fake_bot)
sys.modules.setdefault('telebot', fake_telebot)
fake_requests = types.ModuleType('requests')
fake_requests.post = MagicMock()
sys.modules.setdefault('requests', fake_requests)
os.environ.setdefault('TOKEN', 'test-only-token')
bot = importlib.import_module('bot')


class ParsingTests(unittest.TestCase):
    SAMPLE = '89775179200 Борис. М Крылатское с/у плиточные работы. Оп Любовь'

    def setUp(self):
        bot._known_phones.clear()
        bot._processed_msgs.clear()
        bot._phone_locks.clear()

    def test_short_message_phone_and_fallback(self):
        self.assertEqual(bot.extract_phones(self.SAMPLE), ['+79775179200'])
        lead, = bot.parse_lead_fallback(self.SAMPLE)
        self.assertEqual(lead['name'], 'Борис')
        self.assertEqual(lead['address'], 'м. Крылатское')
        self.assertIn('плиточные работы', lead['work_volume'])

    def test_multiline_contact_details_without_ai(self):
        raw = ('Замер на завтра 19.11\nГерман +7 916 683-21-25\n'
               'Москва Пятницкая улица, 8\nвтор кап 100кв\nбез проживания')
        lead, = bot.parse_lead_fallback(raw)
        self.assertEqual(lead['name'], 'Герман')
        self.assertEqual(lead['address'], 'Москва Пятницкая улица, 8')
        self.assertEqual(lead['deadline'], '19.11')

    def test_phone_formats(self):
        msg = '+7 (977) 517-92-00; 8 977 517 92 00'
        self.assertEqual(bot.extract_phones(msg), ['+79775179200'])

    def test_source_from_operator_for_unknown_chat(self):
        self.assertEqual(bot.get_source_from_message('Общая группа', self.SAMPLE),
                         ('UC_0RYYOV', 'Трафик Любовь'))

    def test_known_group_source_has_priority(self):
        self.assertEqual(bot.get_source_from_message('Авито', self.SAMPLE),
                         ('REPEAT_SALE', 'Реклама Авито'))

    def test_no_phone_not_a_lead(self):
        self.assertEqual(bot.extract_phones('Борис, плитка в санузле'), [])

    def test_status_updates_and_chat_are_not_new_leads(self):
        messages = [
            '89775179200 Борис перезвонить завтра, санузел',
            '89775179200 выезд состоялся, ремонт ванной',
            '89775179200 Борис оплатил смету',
            '89775179200 Борис привет как дела',
        ]
        for message in messages:
            with self.subTest(message=message):
                self.assertFalse(bot.is_new_lead_message(message))

    def test_valid_short_lead_passes_filter(self):
        self.assertTrue(bot.is_new_lead_message(self.SAMPLE))

    def test_group_message_falls_back_when_ai_fails(self):
        message = types.SimpleNamespace(
            chat=types.SimpleNamespace(id=-123, title='Общая группа'),
            message_id=55, text=self.SAMPLE,
            from_user=types.SimpleNamespace(is_bot=False),
        )
        with patch.object(bot, 'parse_lead_ai', return_value=[]), \
             patch.object(bot, 'send_to_bitrix', return_value=(123, 'ok', '')) as send, \
             patch.object(bot, 'set_reaction') as reaction:
            bot.handle_message(message)
        self.assertEqual(send.call_args.args[0]['phone'], '+79775179200')
        self.assertEqual(send.call_args.args[1], 'UC_0RYYOV')
        reaction.assert_called_once()

    def test_failed_lead_does_not_enter_duplicate_cache(self):
        with patch.object(bot, 'check_duplicate', return_value=False), \
             patch.object(bot, 'bitrix_post', side_effect=[{'result': 42}, {'error': 'failed'}]), \
             patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89775179200', 'name': 'Борис',
            }, 'OTHER', 'Telegram', 'Общая группа')
        self.assertIsNone(lead_id)
        self.assertEqual(status, 'bitrix_error')
        self.assertFalse(bot.is_phone_known('+79775179200'))

    def test_duplicate_lookup_failure_aborts_creation(self):
        with patch.object(bot, 'bitrix_post', return_value={}), \
             self.assertRaisesRegex(RuntimeError, 'Не удалось проверить'):
            bot.check_duplicate('+79775179200')


if __name__ == '__main__':
    unittest.main()
