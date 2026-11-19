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

    def _connect(self):
        return sqlite3.connect(self.path, timeout=15)

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

