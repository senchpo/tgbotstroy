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


if __name__ == '__main__':
    unittest.main()
