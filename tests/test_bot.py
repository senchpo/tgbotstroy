import importlib
import os
import sys
import types
import unittest
import tempfile
from unittest.mock import MagicMock, patch
from delivery_queue import DeliveryQueue


# Test parsing and routing without a live Telegram token or network calls.
fake_telebot = types.ModuleType('telebot')
fake_bot = MagicMock()
fake_bot.message_handler.side_effect = lambda **kwargs: (lambda function: function)
fake_bot.callback_query_handler.side_effect = lambda **kwargs: (lambda function: function)
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
        self.queue_dir = tempfile.TemporaryDirectory()
        self.previous_queue = bot._delivery_queue
        bot._delivery_queue = DeliveryQueue(os.path.join(self.queue_dir.name, 'pending.sqlite3'))

    def tearDown(self):
        bot._delivery_queue = self.previous_queue
        self.queue_dir.cleanup()

    def test_short_message_phone_and_fallback(self):
        self.assertEqual(bot.extract_phones(self.SAMPLE), ['+79775179200'])
        lead, = bot.parse_lead_fallback(self.SAMPLE)
        self.assertEqual(lead['name'], 'Борис')
        self.assertEqual(lead['address'], 'м. Крылатское')
        self.assertIn('плиточные работы', lead['work_volume'])

    def test_phone_only_and_missing_fields_never_raise(self):
        for raw in ('89775179200', '+7 (977) 517-92-00',
                    '8 977 517 92 00.', '89775179200 Борис',
                    'Борис 89775179200', '89775179200 кухня',
                    '89775179200\n', '\n89775179200'):
            with self.subTest(raw=raw):
                lead, = bot.parse_lead_fallback(raw)
                self.assertEqual(lead['phone'], '+79775179200')
                self.assertTrue(lead['name'])
                self.assertTrue(lead['address'])

    def test_phone_only_event_queued_and_sent(self):
        message = types.SimpleNamespace(
            chat=types.SimpleNamespace(id=1234, title=None),
            message_id=567, text='89775179200',
            from_user=types.SimpleNamespace(is_bot=False),
        )
        with patch.object(bot, 'send_to_bitrix', return_value=(123, 'ok', '')) as send, \
             patch.object(bot, 'set_reaction'):
            bot.handle_message(message)
        self.assertEqual(send.call_args.args[0]['name'], 'Не указано')
        self.assertEqual(send.call_args.args[1], 'OTHER')
        self.assertEqual(bot._delivery_queue.ready(), [])

    def test_victor_and_service_group_requests_are_delivered(self):
        samples = (
            ('Заявки Виктор', '+79539777262 стены/потолки/обои в 2к кв'),
            ('Отдел сервиса и мелкого ремонта',
             'Валентина +7 980 558-73-66\nМытищи\n'
             'втор косм 300-350кв по стенам, частичная подготовка под обои\n'
             'просит скинуть смс, куда можно будет перезвонить'),
        )
        for index, (title, raw) in enumerate(samples):
            with self.subTest(title=title):
                message = types.SimpleNamespace(
                    chat=types.SimpleNamespace(id=-1000 - index, title=title),
                    message_id=42 + index, text=raw,
                    from_user=types.SimpleNamespace(is_bot=False),
                )
                with patch.object(bot, 'send_to_bitrix', return_value=(999, 'ok', '')) as send, \
                     patch.object(bot, 'set_reaction'):
                    bot.handle_message(message)
                self.assertEqual(send.call_count, 1)
                self.assertEqual(send.call_args.args[0]['raw_text'], raw)
                self.assertEqual(bot._delivery_queue.ready(), [])

    def test_group_request_with_existing_crm_contact_is_rejected_and_acked(self):
        message = types.SimpleNamespace(
            chat=types.SimpleNamespace(id=-112, title='Трафик Любовь'),
            message_id=330, text='Иван. КП Дубрава. Обои, с/у 89663887744',
            from_user=types.SimpleNamespace(is_bot=False),
        )
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'bitrix_post', return_value={
                 'result': {'CONTACT': [7654]}
             }) as post, patch.object(bot, 'set_reaction') as reaction, \
             patch.object(bot.time, 'sleep'):
            bot.handle_message(message)
        self.assertEqual(bot._delivery_queue.ready(), [])
        post.assert_called_once()
        self.assertEqual(post.call_args.args[0], 'crm.duplicate.findByComm')
        reaction.assert_called_once_with(-112, 330, '❌')

    def test_ambiguous_request_is_reviewed_and_not_auto_sent(self):
        message = types.SimpleNamespace(
            chat=types.SimpleNamespace(id=-1000, title='Заявки Виктор'),
            message_id=44, text='Мария +7 916 123-45-67, Москва, нужен расчёт',
            from_user=types.SimpleNamespace(is_bot=False),
        )
        with patch.object(bot, 'REVIEW_ADMIN_CHAT_ID', 123), \
             patch.object(bot, 'send_review_notifications') as notify, \
             patch.object(bot, 'send_to_bitrix') as send:
            bot.handle_message(message)
            bot.handle_message(message)
        send.assert_not_called()
        self.assertEqual(notify.call_count, 2)
        reviews = bot._delivery_queue.reviews_to_notify()
        self.assertEqual(len(reviews), 1)
        self.assertIn('Мария', reviews[0][2])

    def test_admin_is_notified_once_with_approval_buttons(self):
        review_id = bot._delivery_queue.add_review(
            -1000, 47, 'Партнёр', 'Ирина +79161234567, Москва, консультация')
        fake_types = types.SimpleNamespace(
            InlineKeyboardMarkup=MagicMock(return_value=MagicMock()),
            InlineKeyboardButton=MagicMock(side_effect=lambda label, **kwargs:
                                           (label, kwargs['callback_data'])),
        )
        with patch.object(bot, 'REVIEW_ADMIN_CHAT_ID', 123), \
             patch.object(bot.telebot, 'types', fake_types, create=True), \
             patch.object(bot.bot, 'send_message') as send:
            bot.send_review_notifications()
            bot.send_review_notifications()
        send.assert_called_once()
        self.assertEqual(send.call_args.args[0], 123)
        self.assertIn('Ирина', send.call_args.args[1])
        self.assertIn('Группа: Партнёр', send.call_args.args[1])
        buttons = fake_types.InlineKeyboardButton.call_args_list
        self.assertEqual([call.kwargs['callback_data'] for call in buttons],
                         [f'rv:a:{review_id}', f'rv:r:{review_id}'])

    def test_failed_admin_dm_remains_for_retry(self):
        bot._delivery_queue.add_review(-1000, 48, 'Партнёр',
                                       'Ирина +79161234567, Москва, консультация')
        fake_types = types.SimpleNamespace(
            InlineKeyboardMarkup=MagicMock(return_value=MagicMock()),
            InlineKeyboardButton=MagicMock(return_value=MagicMock()),
        )
        with patch.object(bot, 'REVIEW_ADMIN_CHAT_ID', 123), \
             patch.object(bot.telebot, 'types', fake_types, create=True), \
             patch.object(bot.bot, 'send_message', side_effect=RuntimeError('failed')):
            bot.send_review_notifications()
        self.assertEqual(bot._delivery_queue.reviews_to_notify(), [])
        with patch('delivery_queue.time.time', return_value=10**15):
            self.assertEqual(len(bot._delivery_queue.reviews_to_notify()), 1)

    def test_approval_is_authorized_queued_and_idempotent(self):
        queue = bot._delivery_queue
        review_id = queue.add_review(-1000, 44, 'Заявки Виктор',
                                     'Мария +7 916 123-45-67, Москва, нужен расчёт')
        event = types.SimpleNamespace(
            id='callback', data=f'rv:a:{review_id}',
            from_user=types.SimpleNamespace(id=123),
            message=types.SimpleNamespace(
                chat=types.SimpleNamespace(id=123), message_id=99),
        )
        with patch.object(bot, 'REVIEW_ADMIN_CHAT_ID', 123), \
             patch.object(bot, 'process_queued_message', return_value=False) as process:
            bot.handle_review_decision(event)
            bot.handle_review_decision(event)
        process.assert_called_once()
        self.assertEqual(len(queue.ready()), 1)
        self.assertTrue(DeliveryQueue(queue.path).is_review_approved(-1000, 44))

    def test_rejected_and_unauthorized_requests_never_reach_crm(self):
        queue = bot._delivery_queue
        review_id = queue.add_review(-1000, 45, 'Партнёр',
                                     'Ирина 89161234567, адрес Москва')
        event = types.SimpleNamespace(
            id='callback', data=f'rv:a:{review_id}',
            from_user=types.SimpleNamespace(id=666),
            message=types.SimpleNamespace(
                chat=types.SimpleNamespace(id=123), message_id=100),
        )
        with patch.object(bot, 'REVIEW_ADMIN_CHAT_ID', 123), \
             patch.object(bot, 'process_queued_message') as process:
            bot.handle_review_decision(event)
            self.assertEqual(len(queue.reviews_to_notify()), 1)
            event.from_user.id = 123
            event.data = f'rv:r:{review_id}'
            bot.handle_review_decision(event)
        process.assert_not_called()
        self.assertEqual(queue.ready(), [])
        self.assertEqual(queue.reviews_to_notify(), [])

    def test_approved_ambiguous_message_retries_without_reclassification(self):
        queue = bot._delivery_queue
        review_id = queue.add_review(-1000, 46, 'Заявки Виктор',
                                     'Мария +7 916 123-45-67, Москва, нужен расчёт')
        self.assertTrue(queue.decide_review(review_id, True, 123)[0])
        with patch.object(bot, 'send_to_bitrix', return_value=(321, 'ok', '')) as send, \
             patch.object(bot, 'set_reaction'):
            bot.retry_pending()
        self.assertEqual(send.call_count, 1)
        self.assertEqual(queue.ready(), [])
        self.assertFalse(DeliveryQueue(queue.path).is_review_approved(-1000, 46))

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

    def test_failed_delivery_replays_from_persistent_queue(self):
        message = types.SimpleNamespace(
            chat=types.SimpleNamespace(id=-124, title='Общая группа'),
            message_id=56, text=self.SAMPLE,
            from_user=types.SimpleNamespace(is_bot=False),
        )
        with tempfile.TemporaryDirectory() as tmp:
            queue = DeliveryQueue(os.path.join(tmp, 'retries.sqlite3'))
            with patch.object(bot, '_delivery_queue', queue), \
                 patch.object(bot, 'send_to_bitrix', side_effect=[
                     (None, 'bitrix_error', ''), (123, 'ok', '')]) as send, \
                 patch.object(bot, 'set_reaction') as reaction:
                bot.handle_message(message)
                self.assertEqual(queue.ready(), [])  # backoff, not discarded
                with patch('delivery_queue.time.time', return_value=10**15):
                    bot.retry_pending()
                self.assertEqual(DeliveryQueue(queue.path).ready(), [])
                self.assertEqual(send.call_count, 2)
                reaction.assert_called_once()

    def test_failed_lead_does_not_enter_duplicate_cache(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'check_duplicate', return_value=False), \
             patch.object(bot, 'find_contact_by_phone', return_value=None), \
             patch.object(bot, 'bitrix_post', side_effect=[{'result': 42}, {'error': 'failed'}]), \
             patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89775179200', 'name': 'Борис',
            }, 'OTHER', 'Telegram', 'Общая группа')
        self.assertIsNone(lead_id)
        self.assertEqual(status, 'bitrix_error')
        self.assertFalse(bot.is_phone_known('+79775179200'))

    def test_same_telegram_event_is_not_created_twice(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=123), \
             patch.object(bot, 'bitrix_post') as post, \
             patch.object(bot.time, 'sleep'):
            _, status, _ = bot.send_to_bitrix({
                'phone': '89775179200', 'request_uid': 'tg_123',
            }, 'OTHER', 'Telegram', 'Общая группа')
        self.assertEqual(status, 'duplicate')
        post.assert_not_called()

    def test_existing_contact_blocks_new_request_even_from_other_partner(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'find_contact_by_phone') as find_contact, \
             patch.object(bot, 'bitrix_post', return_value={
                 'result': {'CONTACT': [42]}
             }) as post, patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89663887744', 'request_uid': 'tg_new_partner',
                'name': 'Иван', 'raw_text': 'Иван. Ремонт 89663887744',
            }, 'UC_0RYYOV', 'Трафик Любовь', 'Трафик Любовь')
        self.assertEqual((lead_id, status), (None, 'duplicate'))
        find_contact.assert_not_called()
        post.assert_called_once_with('crm.duplicate.findByComm', {
            'type': 'PHONE',
            'values': ['79663887744', '89663887744', '+79663887744'],
            'entity_type': 'CONTACT',
        })
        self.assertFalse(bot._delivery_queue.phone_prechecked('tg_new_partner'))

    def test_existing_lead_without_contact_still_blocks_request(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'find_contact_by_phone') as find_contact, \
             patch.object(bot, 'bitrix_post', side_effect=[
                 {'result': []}, {'result': {'LEAD': [8765]}}
             ]) as post, patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '+7 966 388-77-44', 'request_uid': 'tg_different',
            }, 'OTHER', 'Telegram', 'Другой партнёр')
        self.assertEqual((lead_id, status), (None, 'duplicate'))
        self.assertEqual([c.args[0] for c in post.call_args_list],
                         ['crm.duplicate.findByComm', 'crm.duplicate.findByComm'])
        find_contact.assert_not_called()

    def test_duplicate_lookup_failure_never_writes_to_crm(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'find_contact_by_phone') as find_contact, \
             patch.object(bot, 'bitrix_post', return_value={}) as post, \
             patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89663887744', 'request_uid': 'tg_retry_later',
            }, 'OTHER', 'Telegram', 'Партнёр')
        self.assertEqual((lead_id, status), (None, 'bitrix_error'))
        self.assertEqual(post.call_count, 1)
        find_contact.assert_not_called()
        self.assertFalse(bot._delivery_queue.phone_prechecked('tg_retry_later'))

    def test_contact_appearing_after_precheck_is_not_reused_on_first_attempt(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'check_duplicate', return_value=False), \
             patch.object(bot, 'find_contact_by_phone', return_value=4321), \
             patch.object(bot, 'bitrix_post') as post, \
             patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89663887744', 'request_uid': 'tg_concurrent_contact',
            }, 'OTHER', 'Telegram', 'Партнёр')
        self.assertEqual((lead_id, status), (None, 'duplicate'))
        post.assert_not_called()
        self.assertFalse(bot._delivery_queue.phone_prechecked('tg_concurrent_contact'))

    def test_precheck_survives_restart_after_contact_created_but_lead_failed(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'check_duplicate', return_value=False) as precheck, \
             patch.object(bot, 'find_contact_by_phone', side_effect=[None, 4321]), \
             patch.object(bot, 'bitrix_post', side_effect=[
                 {'result': 4321}, {'error': 'lead temporarily unavailable'},
                 {'result': 8765}, {'result': {'COMMENTS': 'test'}},
             ]) as post, patch.object(bot.time, 'sleep'):
            lead = {'phone': '89663887744', 'name': 'Иван',
                    'request_uid': 'tg_partial_delivery'}
            first = bot.send_to_bitrix(lead, 'OTHER', 'Telegram', 'Партнёр')
            self.assertEqual(first[:2], (None, 'bitrix_error'))
            self.assertTrue(bot._delivery_queue.phone_prechecked('tg_partial_delivery'))
            bot._delivery_queue = DeliveryQueue(bot._delivery_queue.path)
            second = bot.send_to_bitrix(lead, 'OTHER', 'Telegram', 'Партнёр')
        self.assertEqual(second[:2], (8765, 'ok'))
        precheck.assert_called_once_with('+79663887744')
        self.assertEqual([c.args[0] for c in post.call_args_list],
                         ['crm.contact.add', 'crm.lead.add',
                          'crm.lead.add', 'crm.lead.get'])
        self.assertTrue(bot.is_phone_known('+79663887744'))

    def test_lead_title_and_comment_preserve_original_telegram_message(self):
        original = '89775179200 Борис. М Крылатское с/у плиточные работы. Оп Любовь'
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'check_duplicate', return_value=False), \
             patch.object(bot, 'find_contact_by_phone', return_value=None), \
             patch.object(bot, 'bitrix_post', side_effect=[
                 {'result': 4440}, {'result': 5610},
                 {'result': {'COMMENTS': original}},
             ]) as post, patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89775179200', 'name': 'Борис',
                'address': 'м. Крылатское', 'work_volume': 'плиточные работы',
                'raw_text': original, 'request_uid': 'tg_456',
            }, 'OTHER', 'Telegram', 'Общая группа')
        self.assertEqual((lead_id, status), (5610, 'ok'))
        contact_fields = post.call_args_list[0].args[1]['fields']
        lead_fields = post.call_args_list[1].args[1]['fields']
        self.assertEqual(contact_fields['COMMENTS'], original)
        self.assertEqual(lead_fields['COMMENTS'], original)
        self.assertEqual(lead_fields['TITLE'], 'Ремонт | +79775179200 | Борис')
        self.assertEqual(lead_fields['UF_CRM_1776153852'], 'tg_456')

    def test_phone_only_title_and_comment_without_missing_fields(self):
        with patch.object(bot, 'find_lead_by_request_uid', return_value=None), \
             patch.object(bot, 'check_duplicate', return_value=False), \
             patch.object(bot, 'find_contact_by_phone', return_value=None), \
             patch.object(bot, 'bitrix_post', side_effect=[
                 {'result': 42}, {'result': 321},
                 {'result': {'COMMENTS': '89775179200'}}
             ]) as post, patch.object(bot.time, 'sleep'):
            lead_id, status, _ = bot.send_to_bitrix({
                'phone': '89775179200', 'name': 'Не указано',
                'raw_text': '89775179200',
            }, 'OTHER', 'Telegram', 'Личные')
        self.assertEqual((lead_id, status), (321, 'ok'))
        fields = post.call_args_list[1].args[1]['fields']
        self.assertEqual(fields['TITLE'], 'Ремонт | +79775179200 | Не указано')
        self.assertEqual(fields['COMMENTS'], '89775179200')

    def test_duplicate_lookup_failure_aborts_creation(self):
        with patch.object(bot, 'bitrix_post', return_value={}), \
             self.assertRaisesRegex(RuntimeError, 'Не удалось проверить'):
            bot.check_duplicate('+79775179200')

    def test_empty_contact_search_result_formats(self):
        for empty in ({}, []):
            with self.subTest(result=empty), \
                 patch.object(bot, 'bitrix_post', return_value={'result': empty}):
                self.assertIsNone(bot.find_contact_by_phone('+79775179200'))


if __name__ == '__main__':
    unittest.main()
