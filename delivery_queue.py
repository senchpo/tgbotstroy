"""Small retry journal for Telegram updates awaiting Bitrix24 delivery.

SQLite persists pending messages across process restarts when its path is on
a persistent volume. Successfully delivered messages are removed immediately.
"""

import sqlite3
import time


class DeliveryQueue:
    def __init__(self, path: str):
        self.path = path
        with self._connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS pending (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                body TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                due_at REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id, message_id)
            )''')
            db.execute('''CREATE TABLE IF NOT EXISTS manual_review (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                body TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                notified_at REAL,
                notify_attempts INTEGER NOT NULL DEFAULT 0,
                notify_due_at REAL NOT NULL DEFAULT 0,
                decided_by INTEGER,
                UNIQUE(chat_id, message_id)
            )''')
            # A CRM contact may be created before the matching lead. Remember
            # that this Telegram request passed phone deduplication *before*
            # writing either object, so retries can finish a partial delivery
            # without mistaking our own new contact for a pre-existing client.
            db.execute('''CREATE TABLE IF NOT EXISTS phone_prechecks (
                request_uid TEXT PRIMARY KEY,
                checked_at REAL NOT NULL
            )''')

    def _connect(self):
        return sqlite3.connect(self.path, timeout=15)

    def phone_prechecked(self, request_uid: str) -> bool:
        if not request_uid:
            return False
        with self._connect() as db:
            return db.execute('''SELECT 1 FROM phone_prechecks
                WHERE request_uid=?''', (request_uid,)).fetchone() is not None

    def mark_phone_prechecked(self, request_uid: str):
        if not request_uid:
            return
        with self._connect() as db:
            db.execute('''INSERT OR IGNORE INTO phone_prechecks
                (request_uid, checked_at) VALUES (?, ?)''',
                (request_uid, time.time()))

    def clear_phone_precheck(self, request_uid: str):
        if not request_uid:
            return
        with self._connect() as db:
            db.execute('DELETE FROM phone_prechecks WHERE request_uid=?',
                       (request_uid,))

    def enqueue(self, chat_id: int, message_id: int, chat_title: str, body: str):
        with self._connect() as db:
            db.execute('''INSERT OR IGNORE INTO pending
                (chat_id, message_id, chat_title, body) VALUES (?, ?, ?, ?)''',
                (chat_id, message_id, chat_title, body))

    def ready(self, limit: int = 20):
        with self._connect() as db:
            return db.execute('''SELECT chat_id, message_id, chat_title, body
                FROM pending WHERE due_at <= ? ORDER BY due_at, rowid LIMIT ?''',
                (time.time(), limit)).fetchall()

    def complete(self, chat_id: int, message_id: int):
        with self._connect() as db:
            db.execute('DELETE FROM pending WHERE chat_id = ? AND message_id = ?',
                       (chat_id, message_id))
            db.execute('''UPDATE manual_review SET state='delivered', body=''
                WHERE chat_id=? AND message_id=? AND state='approved' ''',
                (chat_id, message_id))

    def defer(self, chat_id: int, message_id: int):
        with self._connect() as db:
            row = db.execute('SELECT attempts FROM pending WHERE chat_id=? AND message_id=?',
                             (chat_id, message_id)).fetchone()
            if row is None:
                return
            attempts = row[0] + 1
            delay = min(3600, 10 * (2 ** min(attempts - 1, 9)))
            db.execute('''UPDATE pending SET attempts=?, due_at=?
                WHERE chat_id=? AND message_id=?''',
                (attempts, time.time() + delay, chat_id, message_id))

    def add_review(self, chat_id: int, message_id: int, chat_title: str,
                   body: str) -> int:
        """Journal a possible lead once; later updates cannot replace its text."""
        with self._connect() as db:
            db.execute('''INSERT OR IGNORE INTO manual_review
                (chat_id, message_id, chat_title, body) VALUES (?, ?, ?, ?)''',
                (chat_id, message_id, chat_title, body))
            row = db.execute('''SELECT id FROM manual_review
                WHERE chat_id=? AND message_id=?''',
                (chat_id, message_id)).fetchone()
            return row[0]

    def reviews_to_notify(self, limit: int = 10):
        with self._connect() as db:
            return db.execute('''SELECT id, chat_title, body FROM manual_review
                WHERE state='pending' AND notified_at IS NULL AND notify_due_at<=?
                ORDER BY id LIMIT ?''', (time.time(), limit)).fetchall()

    def review_notified(self, review_id: int):
        with self._connect() as db:
            db.execute('''UPDATE manual_review SET notified_at=?
                WHERE id=? AND state='pending' AND notified_at IS NULL''',
                (time.time(), review_id))

    def review_notify_failed(self, review_id: int):
        with self._connect() as db:
            row = db.execute('''SELECT notify_attempts FROM manual_review
                WHERE id=? AND state='pending' ''', (review_id,)).fetchone()
            if row is None:
                return
            attempts = row[0] + 1
            delay = min(3600, 10 * (2 ** min(attempts - 1, 9)))
            db.execute('''UPDATE manual_review
                SET notify_attempts=?, notify_due_at=? WHERE id=?''',
                (attempts, time.time() + delay, review_id))

    def decide_review(self, review_id: int, approve: bool,
                      admin_id: int) -> tuple[bool, tuple | None]:
        """Atomically approve+queue or reject; duplicate callbacks are inert."""
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT chat_id, message_id, chat_title, body, state
                FROM manual_review WHERE id=?''', (review_id,)).fetchone()
            if row is None or row[4] != 'pending':
                return False, row
            chat_id, message_id, chat_title, body, _ = row
            if approve:
                db.execute('''INSERT OR IGNORE INTO pending
                    (chat_id, message_id, chat_title, body)
                    VALUES (?, ?, ?, ?)''',
                    (chat_id, message_id, chat_title, body))
            db.execute('''UPDATE manual_review SET state=?, decided_by=?, body=?
                WHERE id=?''', ('approved' if approve else 'rejected', admin_id,
                                body if approve else '', review_id))
            return True, row

    def is_review_approved(self, chat_id: int, message_id: int) -> bool:
        with self._connect() as db:
            return db.execute('''SELECT 1 FROM manual_review
                WHERE chat_id=? AND message_id=? AND state='approved' ''',
                (chat_id, message_id)).fetchone() is not None

