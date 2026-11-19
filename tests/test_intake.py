import unittest

from intake import classify_message, event_uid, phones_in, needs_manual_review


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
        '+79539777262 стены/потолки/обои в 2к кв',
        ('Валентина +7 980 558-73-66\nМытищи\nвтор косм 300-350кв по стенам\n'
         'частичная подготовка стен под обои и поклейка обоев\n'
         'просит скинуть смс с номером, куда можно будет перезвонить\n'
         'и договориться о встрече со сметчиком'),
        ('Анна 7 916 602-51-88\nКоньково\nвтор кап косм 48кв\n'
         'сегодня будет советоваться с сестрой по поводу встречи с инженером\n'
         'звонить завтра, во второй половине дня'),
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

    def test_minimal_requests_and_statuses(self):
        for value in ('89775179200', '+7 (977) 517-92-00',
                      '8 977 517 92 00.', '89775179200 Борис',
                      'Борис 89775179200'):
            with self.subTest(value=value):
                self.assertEqual(classify_message(value).kind, 'new_request')
        for value in ('89775179200 привет', '89775179200 новый номер',
                      '89775179200 перезвонить', '89775179200 отказ'):
            with self.subTest(value=value):
                self.assertNotEqual(classify_message(value).kind, 'new_request')

    def test_only_plausible_ambiguous_leads_are_escalated(self):
        for message in (
            'Мария +7 916 123-45-67, Москва, нужен расчёт',
            'Анна 89161234567, м. Молодёжная, консультация',
            '89161234567 установка ламината, сроки и стоимость',
        ):
            with self.subTest(message=message):
                self.assertEqual(classify_message(message).kind, 'ambiguous')
                self.assertTrue(needs_manual_review(message))
        for message in (
            '89161234567 привет',
            '89161234567 новый номер',
            '89161234567 не берёт трубку',
            '89161234567 ремонт кухни, перезвонить',
            '89161234567',
            'Всем привет без номера',
        ):
            with self.subTest(message=message):
                self.assertFalse(needs_manual_review(message))

    def test_stress_10000_mixed_messages_without_exceptions(self):
        for i in range(10000):
            phone = f'89{i:09d}'
            kind = i % 4
            message = (phone if kind == 0 else
                       phone + ' Иван' if kind == 1 else
                       phone + ' ремонт кухни' if kind == 2 else
                       phone + ' недозвон')
            decision = classify_message(message)
            self.assertEqual(decision.kind, 'status_update' if kind == 3 else 'new_request')


if __name__ == '__main__':
    unittest.main()
