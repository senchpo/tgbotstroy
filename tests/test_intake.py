import unittest

from intake import classify_message, event_uid, phones_in


class IntakeClassifierTests(unittest.TestCase):
    NEW = [
        '89775179200 Борис. М Крылатское с/у плиточные работы. Оп Любовь',
        'Мария 8 903 123-45-67 ремонт кухни',
        '+7 (916) 111-22-33 ванная под ключ',
        '89161234567 плитка в санузле',
        'Заявка 89161234567 Иван',
        '89161234567 замер завтра, адрес Пушкина 1',
        '89161234567 втор кап 100кв',
        '89161234567 отделка студии',
        '89161234567 покраска стен',
        '89161234567 сантехника',
        '89161234567 электрика',
        'Новый клиент 89161234567 хочет ремонт, перезвонить',
        '89161234567 кухня 12 м2',
        '89161234567 ремонт балкона',
        '89161234567 штукатурка',
    ]
    STATUS = [
        '89161234567 ремонт кухни, перезвонить',
        '89161234567 недозвон санузел',
        '89161234567 смета отправлена по квартире',
        '89161234567 выезд состоялся, ремонт',
        '89161234567 замер состоялся, ремонт',
        '89161234567 не актуально, ремонт',
        '89161234567 отказ от ремонта',
        '89161234567 договор подписан, кухня',
        '89161234567 в работе, санузел',
        '89161234567 оплатил ремонт',
        '89161234567 ремонт закрыт',
        '89161234567 отложено по кухне',
    ]
    AMBIGUOUS = [
        '89161234567 привет',
        '89161234567 всё хорошо',
        '89161234567 созвон с Борисом',
        '89161234567 новый номер',
    ]
    IGNORE = [
        'Перезвонить по заявке без номера',
        'Смета готова',
        'Всем привет',
        'Клиент хочет санузел',
    ]

    def test_table_of_realistic_group_messages(self):
        for expected, samples in [
            ('new_request', self.NEW),
            ('status_update', self.STATUS),
            ('ambiguous', self.AMBIGUOUS),
            ('ignore', self.IGNORE),
        ]:
            for sample in samples:
                with self.subTest(message=sample):
                    self.assertEqual(classify_message(sample).kind, expected)

    def test_phone_canonicalization_and_unique(self):
        self.assertEqual(phones_in('8 977 517 92 00, +7(977)517-92-00'),
                         ('+79775179200',))

    def test_event_uid_is_stable_but_distinct(self):
        a = event_uid(-100, 123, '+79775179200')
        self.assertEqual(a, event_uid(-100, 123, '+79775179200'))
        self.assertNotEqual(a, event_uid(-100, 124, '+79775179200'))
        self.assertNotEqual(a, event_uid(-101, 123, '+79775179200'))


if __name__ == '__main__':
    unittest.main()
