import os
import tempfile
import unittest
from unittest.mock import patch

from delivery_queue import DeliveryQueue


class DeliveryQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'events.sqlite3')

    def tearDown(self):
        self.tmp.cleanup()

    def test_event_persists_restart_and_removes_after_success(self):
        queue = DeliveryQueue(self.path)
        queue.enqueue(-100, 7, 'Заявки', '89161234567 ремонт кухни')
        queue.enqueue(-100, 7, 'Заявки', 'duplicate')
        reopened = DeliveryQueue(self.path)
        self.assertEqual(reopened.ready(),
                         [(-100, 7, 'Заявки', '89161234567 ремонт кухни')])
        reopened.complete(-100, 7)
        self.assertEqual(DeliveryQueue(self.path).ready(), [])

    def test_failure_is_deferred_and_event_is_not_lost(self):
        queue = DeliveryQueue(self.path)
        queue.enqueue(-100, 8, 'Заявки', '89161234567 ремонт')
        with patch('delivery_queue.time.time', return_value=1000):
            queue.defer(-100, 8)
            self.assertEqual(queue.ready(), [])
        with patch('delivery_queue.time.time', return_value=1011):
            self.assertEqual(len(queue.ready()), 1)
        with patch('delivery_queue.time.time', return_value=1011):
            queue.defer(-100, 8)
        with patch('delivery_queue.time.time', return_value=1030):
            self.assertEqual(queue.ready(), [])
        with patch('delivery_queue.time.time', return_value=1032):
            self.assertEqual(len(queue.ready()), 1)

    def test_stress_2000_events_reopen_and_replay(self):
        queue = DeliveryQueue(self.path)
        for i in range(2000):
            queue.enqueue(-100, i, 'Партнёр', f'89{i:09d}')
        reopened = DeliveryQueue(self.path)
        count = 0
        while batch := reopened.ready(limit=137):
            for chat_id, message_id, _, _ in batch:
                reopened.complete(chat_id, message_id)
                count += 1
        self.assertEqual(count, 2000)
        self.assertEqual(DeliveryQueue(self.path).ready(), [])

    def test_review_is_durable_deduplicated_and_approved_atomically(self):
        queue = DeliveryQueue(self.path)
        review_id = queue.add_review(-100, 12, 'Партнёр',
                                     'Анна +79161234567, Москва, консультация')
        self.assertEqual(queue.add_review(-100, 12, 'Партнёр', 'изменённый текст'),
                         review_id)
        reopened = DeliveryQueue(self.path)
        self.assertEqual(len(reopened.reviews_to_notify()), 1)
        accepted, row = reopened.decide_review(review_id, True, 123)
        self.assertTrue(accepted)
        self.assertIn('Анна', row[3])
        self.assertEqual(reopened.ready(), [
            (-100, 12, 'Партнёр', 'Анна +79161234567, Москва, консультация')
        ])
        self.assertFalse(reopened.decide_review(review_id, False, 123)[0])
        reopened.complete(-100, 12)
        self.assertEqual(DeliveryQueue(self.path).ready(), [])
        self.assertEqual(reopened.reviews_to_notify(), [])

    def test_rejected_review_does_not_queue_crm_request(self):
        queue = DeliveryQueue(self.path)
        review_id = queue.add_review(-100, 13, 'Партнёр', 'Ирина +79161234567')
        self.assertTrue(queue.decide_review(review_id, False, 123)[0])
        self.assertEqual(DeliveryQueue(self.path).ready(), [])
        self.assertFalse(queue.decide_review(review_id, True, 123)[0])

    def test_failed_admin_notification_is_retried_not_forgotten(self):
        queue = DeliveryQueue(self.path)
        review_id = queue.add_review(-100, 14, 'Партнёр', 'Москва, телефон +79161234567')
        with patch('delivery_queue.time.time', return_value=1000):
            queue.review_notify_failed(review_id)
            self.assertEqual(queue.reviews_to_notify(), [])
        with patch('delivery_queue.time.time', return_value=1011):
            self.assertEqual(len(DeliveryQueue(self.path).reviews_to_notify()), 1)
            queue.review_notified(review_id)
            self.assertEqual(queue.reviews_to_notify(), [])

    def test_stress_1000_reviews_survive_restart_and_exactly_one_decision(self):
        queue = DeliveryQueue(self.path)
        for i in range(1000):
            queue.add_review(-100, i + 10000, 'Партнёр',
                             f'Мария +79161234567, консультация {i}')
        reopened = DeliveryQueue(self.path)
        self.assertEqual(len(reopened.reviews_to_notify(limit=1001)), 1000)
        for review_id in range(1, 1001):
            approved = review_id % 2 == 0
            self.assertTrue(reopened.decide_review(review_id, approved, 123)[0])
            self.assertFalse(reopened.decide_review(review_id, not approved, 123)[0])
        self.assertEqual(len(DeliveryQueue(self.path).ready(limit=1000)), 500)
        self.assertEqual(DeliveryQueue(self.path).reviews_to_notify(limit=1001), [])


if __name__ == '__main__':
    unittest.main()
