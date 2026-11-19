import telebot
import requests
import time
import os
import re
import threading
from datetime import datetime
from intake import classify_message, event_uid, needs_manual_review
from delivery_queue import DeliveryQueue

# ============================================
# КОНФИГ
# ============================================
TOKEN        = os.environ.get("TOKEN", "")
BITRIX_URL   = os.environ.get("BITRIX_URL", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
USE_AI_PARSER = os.environ.get('USE_AI_PARSER', 'false').lower() == 'true'
QUEUE_DB_PATH = os.environ.get('QUEUE_DB_PATH', 'telegram_pending.sqlite3')
ADMIN_USER_IDS = {int(x) for x in os.environ.get('ADMIN_USER_IDS', '').split(',')
                  if x.strip().isdigit()}
_review_admin_setting = os.environ.get('REVIEW_ADMIN_CHAT_ID', '').strip()
# Only a single, explicitly selected Telegram account receives customer PII.
REVIEW_ADMIN_CHAT_ID = (int(_review_admin_setting)
                        if _review_admin_setting.isdigit()
                        else next(iter(ADMIN_USER_IDS)) if len(ADMIN_USER_IDS) == 1
                        else None)

# ============================================
# БЛОКИРОВКИ ПО ТЕЛЕФОНУ
# ============================================
_phone_locks: dict[str, datetime] = {}
_locks_mutex = threading.Lock()
LOCK_TTL = 60


def acquire_lock(phone: str) -> bool:
    with _locks_mutex:
        now = datetime.now()
        expired = [p for p, t in _phone_locks.items()
                   if (now - t).total_seconds() > LOCK_TTL]
        for p in expired:
            del _phone_locks[p]

        if phone in _phone_locks:
            age = (now - _phone_locks[phone]).total_seconds()
            print(f"🔒 Номер уже обрабатывается ({age:.0f}с)")
            return False

        _phone_locks[phone] = now
        print('🔓 Захвачена блокировка номера')
        return True


def release_lock(phone: str):
    with _locks_mutex:
        _phone_locks.pop(phone, None)
        print('🔓 Снята блокировка номера')


# ============================================
# ЛОКАЛЬНЫЙ КЭШ ТЕЛЕФОНОВ
# ============================================
_known_phones: set[str] = set()
_known_phones_mutex = threading.Lock()


def is_phone_known(phone: str) -> bool:
    with _known_phones_mutex:
        return phone in _known_phones


def mark_phone_known(phone: str):
    with _known_phones_mutex:
        _known_phones.add(phone)
        print('📝 Номер добавлен в кэш')


# ============================================
# КАРТЫ ИСТОЧНИКОВ
# ============================================
CHAT_SOURCE_MAP = {
    'авито':        ('REPEAT_SALE', 'Реклама Авито'),
    'прагмат':      ('STORE',       'Прагматремонт(Тимур)'),
    'планремонт':   ('BOOKING',     'Планремонта(Денис)'),
    'САВ':          ('BOOKING',     'Планремонта(Денис)'),
    'ремонкаждому': ('CALL',        'РемонКаждому(Марина)'),
    'каждому':      ('CALL',        'РемонКаждому(Марина)'),
    'партнерка':    ('CALL',        'РемонКаждому(Марина)'),
    'реклама':      ('WEBFORM',     'Реклама(Владимир)'),
    'рбт':          ('CALLBACK',    'РБТ(Сергей)'),
    'Алексей':          ('UC_8A27ZR', 'Заявки Алексей'),
    'Виктор':          ('UC_PDA5W4', 'Виктор заявки'),
    'Жунус':          ('UC_FC89WH', 'Жунус Заявки'),
    'Диана':          ('UC_PMQORN', 'Заявки Диана'),
    'Натальи':          ('UC_XY76YG', 'Трафик от Натальи'),
    'Артёма':          ('UC_BQGUO3', 'Трафик от Артёма'),
    'Любовь':          ('UC_0RYYOV', 'Трафик Любовь'),
    'Александр':          ('UC_KVL6RW', 'Трафик Александр')
}

REPAIR_TYPE_MAP = {
    "Ремонт сан узла Капитальный":      "SALE",
    "Ремонт сан узла Косметический":    "COMPLEX",
    "Ремонт кухни Капитальный":         "GOODS",
    "Ремонт кухни Косметический":       "SERVICES",
    "Ремонт комнаты Капитальный":       "SERVICE",
    "Ремонт комнаты Косметический":     "UC_0CYE7C",
    "Ремонт балкона":                   "UC_VE89TO",
    "Ремонт студия косметический":      "UC_GZQX7R",
    "Ремонт студия капитальный":        "UC_D3H2SP",
    "1-комнатная ремонт капитальный":   "UC_0S6L22",
    "1-комнатная ремонт косметический": "UC_WQOUGR",
    "2-комнатная ремонт капитальный":   "UC_0BIGVY",
    "2-комнатная ремонт косметический": "UC_D316FF",
    "3 и более ремонт капитальный":     "UC_MHOYRF",
    "3 и более ремонт косметический":   "UC_37U9HQ",
    "Ремонт дома капитальный":          "UC_E0Y95B",
    "Ремонт дома косметический":        "UC_HJ62NC",
    "Ремонт офис капитальный":          "UC_EISNN2",
    "Ремонт офис косметический":        "UC_JV05SS",
}

KEYWORDS = [
    'ремонт', 'кв.м', 'м2', 'квартира', 'квартиру',
    '+7', 'санузел', 'сан узел', 'кухня', 'балкон',
    'студия', 'комната', 'однушка', 'двушка', 'трёшка',
    'коттедж', 'косметический', 'капитальный', 'замер',
    'покраска', 'штукатурка', 'вторичка', 'новостройка',
    'под ключ', 'чистовая', 'черновая', 'офис',
    'столешница', 'витрина', 'помещение', 'магазин',
    'с/у', 'с.у.', 'плитка', 'плиточные', 'плиточник',
    'ванна', 'туалет', 'заявка', 'отделка', 'электрика',
]

STATUS_MARKERS = (
    'не дозвони', 'недозвон', 'перезвон', 'не актуал', 'неактуал',
    'отказ', 'смета отправ', 'выезд состоял', 'замер состоял',
    'договор подпис', 'в работе', 'закрыт', 'оплат',
)

PHONE_PATTERN = re.compile(
    r'(?<!\d)(?:\+7|7|8)[\s().-]*\d{3}[\s().-]*\d{3}'
    r'[\s().-]*\d{2}[\s().-]*\d{2}(?!\d)'
)


def extract_phones(text: str) -> list[str]:
    """Find Russian mobile/landline numbers without relying on repair keywords."""
    return list(dict.fromkeys(normalize_phone(m.group()) for m in PHONE_PATTERN.finditer(text)))


def is_new_lead_message(text: str) -> bool:
    """Require a phone AND a work/request signal; reject obvious status posts."""
    lowered = text.casefold()
    if not extract_phones(text):
        return False
    if any(marker in lowered for marker in STATUS_MARKERS):
        return False
    return any(keyword in lowered for keyword in KEYWORDS)


def parse_lead_fallback(text: str) -> list[dict]:
    """Keep short, phone-bearing requests usable when the AI is unavailable."""
    phones = extract_phones(text)
    lowered = text.casefold()
    kind = ('Капитальный' if any(x in lowered for x in ('капитальн', 'под ключ'))
            else 'Косметический' if 'косметическ' in lowered else None)
    room = ('сан узла' if any(x in lowered for x in ('с/у', 'с.у.', 'сануз', 'сан уз', 'ванн', 'туалет'))
            else 'кухни' if 'кухн' in lowered else 'комнаты' if 'комнат' in lowered else None)
    category = f'Ремонт {room} {kind}' if room and kind else ''
    leads = []
    for phone in phones:
        raw_match = next((m for m in PHONE_PATTERN.finditer(text)
                          if normalize_phone(m.group()) == phone), None)
        suffix = text[raw_match.end():].strip(' .,;:-') if raw_match else ''
        first_word = re.match(r'([А-ЯЁ][а-яё]+)\b', suffix)
        if not first_word and raw_match:
            prefix_lines = text[:raw_match.start()].splitlines()
            prefix = prefix_lines[-1].strip(' .,;:-') if prefix_lines else ''
            first_word = re.search(r'([А-ЯЁ][а-яё]+)\s*$', prefix)
        name = first_word.group(1) if first_word else 'Не указано'
        metro = re.search(r'\b[мМ]\.?\s+([А-ЯЁа-яё][А-ЯЁа-яё-]+)', text)
        address = f'м. {metro.group(1)}' if metro else 'Не указано'
        if not metro:
            for line in text.splitlines():
                line = line.strip()
                if re.search(r'\b(улица|ул\.|проспект|пр-т|шоссе|переулок|д\.)\b',
                             line, re.IGNORECASE) and not PHONE_PATTERN.search(line):
                    address = line[:200]
                    break
        date_match = re.search(r'\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b', text)
        leads.append({
            'phone': phone, 'name': name, 'address': address,
            'work_volume': text, 'deadline': date_match.group() if date_match else 'Не указано',
            'category': category, 'comment': 'Автоматический разбор без ИИ',
            'raw_text': text,
        })
    return leads

_processed_msgs: set[str] = set()  # compatibility with older tests
_processed_msgs_mutex = threading.Lock()
_queue_mutex = threading.RLock()
_delivery_queue = None


def get_delivery_queue():
    global _delivery_queue
    if _delivery_queue is None:
        _delivery_queue = DeliveryQueue(QUEUE_DB_PATH)
    return _delivery_queue


# ============================================
# ВСПОМОГАТЕЛЬНЫЕ
# ============================================
def normalize_phone(phone: str) -> str:
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    return '+' + digits if digits.startswith('7') else digits


def get_source_from_chat(chat_title: str) -> tuple[str, str]:
    if not chat_title:
        return 'OTHER', 'Telegram'
    chat_lower = chat_title.lower().replace(' ', '')
    for kw, (src_id, src_name) in CHAT_SOURCE_MAP.items():
        if kw.lower() in chat_lower:
            return src_id, src_name
    return 'OTHER', f'Telegram: {chat_title}'


def get_source_from_message(chat_title: str, text: str) -> tuple[str, str]:
    """Use an explicit operator tag only when the group has no known source."""
    source_id, source_name = get_source_from_chat(chat_title)
    if source_id != 'OTHER':
        return source_id, source_name
    operator = re.search(r'\b[Оо]п\.?\s+([А-ЯЁа-яё]+)', text)
    if operator:
        operator_name = operator.group(1).lower()
        for key, value in CHAT_SOURCE_MAP.items():
            if key.lower() == operator_name:
                return value
    return source_id, source_name


def get_type_id(category: str) -> str | None:
    if not category:
        return None
    cat_lower = category.lower().strip()
    for name, tid in REPAIR_TYPE_MAP.items():
        if name.lower() == cat_lower:
            return tid
    for name, tid in REPAIR_TYPE_MAP.items():
        if cat_lower in name.lower():
            return tid
    return None


def bitrix_post(method: str, payload: dict) -> dict:
    url = BITRIX_URL.rstrip('/') + '/' + method
    try:
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json()

        if isinstance(data, list):
            print(f"⚠️ [{method}] вернул список: {data}")
            return {"result": data}

        if not isinstance(data, dict):
            print(f"⚠️ [{method}] неожиданный тип: {type(data)} = {data}")
            return {}

        if 'error' in data:
            print(f"⚠️ Битрикс ошибка [{method}]: "
                  f"{data['error']} — {data.get('error_description', '')}")

        return data

    except Exception as e:
        print(f"❌ Ошибка запроса [{method}]: {e}")
        return {}


# ============================================
# ПРОВЕРКА ДУБЛЕЙ
# ============================================
def check_duplicate(phone_norm: str) -> bool:
    if is_phone_known(phone_norm):
        print(f"⛔ [КЭШ] Дубль: {phone_norm}")
        return True

    phone_digits = re.sub(r'\D', '', phone_norm)
    print('🔍 Проверяем дубль по номеру')

    resp = bitrix_post("crm.duplicate.findByComm", {
        "type":        "PHONE",
        "values":      [phone_digits],
        "entity_type": "CONTACT",
    })
    if 'result' not in resp or 'error' in resp:
        raise RuntimeError('Не удалось проверить дубли контактов')
    result = resp.get("result", {})
    if isinstance(result, list):
        result = {}
    print(f'  Совпадений контактов: {len(result.get("CONTACT", []))}')
    if isinstance(result, dict) and result.get("CONTACT"):
        print('⛔ Найден существующий контакт')
        mark_phone_known(phone_norm)
        return True

    resp2 = bitrix_post("crm.duplicate.findByComm", {
        "type":        "PHONE",
        "values":      [phone_digits],
        "entity_type": "LEAD",
    })
    if 'result' not in resp2 or 'error' in resp2:
        raise RuntimeError('Не удалось проверить дубли лидов')
    result2 = resp2.get("result", {})
    if isinstance(result2, list):
        result2 = {}
    print(f'  Совпадений лидов: {len(result2.get("LEAD", []))}')
    if isinstance(result2, dict) and result2.get("LEAD"):
        print('⛔ Найден существующий лид')
        mark_phone_known(phone_norm)
        return True

    print('✅ Совпадений нет')
    return False


REQUEST_UID_FIELD = 'UF_CRM_1776153852'  # Verified via production READ-ONLY REST metadata


def find_lead_by_request_uid(request_uid: str) -> int | None:
    """Deduplicate an event, not a customer phone number."""
    if not request_uid:
        return None
    result = bitrix_post('crm.lead.list', {
        'filter': {REQUEST_UID_FIELD: request_uid},
        'select': ['ID', REQUEST_UID_FIELD],
    })
    if 'error' in result or not isinstance(result.get('result'), list):
        raise RuntimeError('Не удалось проверить UID заявки')
    for item in result['result']:
        if item.get(REQUEST_UID_FIELD) == request_uid:
            return int(item['ID'])
    return None


def find_contact_by_phone(phone_norm: str) -> int | None:
    digits = re.sub(r'\D', '', phone_norm)
    result = bitrix_post('crm.duplicate.findByComm', {
        'type': 'PHONE', 'values': [digits], 'entity_type': 'CONTACT',
    })
    if 'error' in result or 'result' not in result:
        raise RuntimeError('Не удалось проверить контакт по телефону')
    data = result['result']
    if isinstance(data, list) and not data:
        return None
    if not isinstance(data, dict):
        raise RuntimeError('Неожиданный формат поиска контакта')
    matches = data.get('CONTACT', [])
    return int(matches[0]) if matches else None


# ============================================
# AI ПАРСИНГ
# ============================================
def parse_lead_ai(text: str) -> list[dict]:
    try:
        categories_list = "\n".join([f"- {k}" for k in REPAIR_TYPE_MAP.keys()])

        payload = {
    "model": "openai/gpt-oss-120b",
    "temperature": 0,
    "messages": [{
        "role": "user",
        "content": f"""/no_think
Ты парсер заявок на ремонт квартир.
Найди всех клиентов и верни СТРОГО в формате:

ЛИД 1:
ИМЯ: ...
ТЕЛЕФОН: ...
АДРЕС: ...
ОБЪЁМ: ...
СРОК: ...
КАТЕГОРИЯ: ...
КОММЕНТАРИЙ: ...
---

ПРАВИЛА:
- ТЕЛЕФОН: формат +7XXXXXXXXXX
- Если нет данных — пиши: Не указано
- Несколько клиентов → ЛИД 1, ЛИД 2 и т.д.
- Отвечай ТОЛЬКО в указанном формате, без рассуждений

КАТЕГОРИЯ — одна строка из списка:
{categories_list}

Правила КАТЕГОРИИ:
- санузел/ванна/туалет → сан узла
- кухня/столешница → кухни
- комната/зал/спальня → комнаты
- балкон/лоджия → балкона
- студия → студия
- однушка/1к/1-комн → 1-комнатная
- двушка/2к/2-комн → 2-комнатная
- трёшка/3к/3-комн/4к+ → 3 и более
- дом/коттедж/дача → дома
- офис/помещение/магазин → офис
- капитальный/под ключ/черновая/евро → Капитальный
- косметический/частичный/покраска → Косметический

Текст заявки:
{text}"""
            }]
        }
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=30
        )
        rj = resp.json()

        if 'error' in rj:
            print(f"❌ Groq: {rj['error']}")
            return []

        ai_text = rj['choices'][0]['message']['content']
        print('🤖 Получен ответ от AI-парсера')

        leads, cur = [], {}

        for line in ai_text.split('\n'):
            line = line.strip()
            if not line:
                continue

            if line.startswith('ЛИД'):
                if cur.get('phone') and cur['phone'] != 'Не указано':
                    leads.append(cur)
                cur = {'raw_text': text}
            elif line.startswith('ИМЯ:'):
                cur['name']        = line[4:].strip()
            elif line.startswith('ТЕЛЕФОН:'):
                cur['phone']       = line[8:].strip()
            elif line.startswith('АДРЕС:'):
                cur['address']     = line[6:].strip()
            elif line.startswith('ОБЪЁМ:'):
                cur['work_volume'] = line[6:].strip()
            elif line.startswith('СРОК:'):
                cur['deadline']    = line[5:].strip()
            elif line.startswith('КАТЕГОРИЯ:'):
                cur['category']    = line[10:].strip()
            elif line.startswith('КОММЕНТАРИЙ:'):
                cur['comment']     = line[12:].strip()
            elif line == '---':
                if cur.get('phone') and cur['phone'] != 'Не указано':
                    leads.append(cur)
                cur = {'raw_text': text}

        if cur.get('phone') and cur['phone'] != 'Не указано':
            leads.append(cur)

        seen, unique = set(), []
        for lead in leads:
            p = normalize_phone(lead.get('phone', ''))
            if p not in seen:
                seen.add(p)
                unique.append(lead)

        print(f"📋 Найдено лидов: {len(unique)}")
        return unique

    except Exception as e:
        print(f"❌ AI ошибка: {e}")
        return []


# ============================================
# СОЗДАНИЕ В БИТРИКС
# ============================================
def send_to_bitrix(data: dict, source_id: str, source_name: str, chat_title: str) -> tuple:
    phone    = data.get('phone', '').strip()
    name     = data.get('name', 'Не указано').strip()
    address  = data.get('address', 'Не указано').strip()
    category = data.get('category', '').strip()
    raw_text = data.get('raw_text', '')
    request_uid = data.get('request_uid', '')

    if not phone or phone == 'Не указано':
        return None, "no_phone", category

    phone_norm = normalize_phone(phone)

    if len(re.sub(r'\D', '', phone_norm)) < 11:
        print('⚠️ Некорректный телефон')
        return None, "no_phone", category

    if not acquire_lock(phone_norm):
        return None, "duplicate", category

    try:
        time.sleep(0.3)

        if find_lead_by_request_uid(request_uid):
            return None, "duplicate", category

        contact_id = find_contact_by_phone(phone_norm)
        title = f"Ремонт | {phone_norm} | {name}"

        # Keep the original Telegram message intact. The request UID is stored
        # separately in the CRM field, not mixed into customer-facing comments.
        comments = raw_text if raw_text.strip() else f"{name}\n{phone_norm}"

        print('📝 Подготовлен комментарий к CRM без вывода персональных данных')

        # ── КОНТАКТ ──────────────────────────────────
        if contact_id:
            print(f'ℹ️ Используем существующий контакт ID:{contact_id}')
        else:
            cr = bitrix_post("crm.contact.add", {"fields": {
                "NAME":               name,
                "PHONE":              [{"VALUE": phone_norm, "VALUE_TYPE": "WORK"}],
                "ADDRESS":            address,
                "SOURCE_ID":          source_id,
                "SOURCE_DESCRIPTION": source_name,
                "COMMENTS":           comments,
            }})
            contact_id = cr.get('result') if isinstance(cr, dict) else None
            if not contact_id:
                raise RuntimeError('Не удалось создать контакт')
            print(f'✅ Контакт создан ID:{contact_id}')

        # ── ЛИД (вместо сделки) ──────────────────────
        lead_fields = {
            "TITLE":              title,
            "NAME":               name,
            "PHONE":              [{"VALUE": phone_norm, "VALUE_TYPE": "WORK"}],
            "ADDRESS":            address,
            "COMMENTS":           comments,
            "SOURCE_ID":          source_id,
            "SOURCE_DESCRIPTION": source_name,
        }

        # TYPE_ID is not available in this portal's crm.lead.fields.
        # TYPE_ID is unavailable in this portal; do not inject parsed metadata
        # into COMMENTS, which must match the original Telegram message.
        if request_uid:
            lead_fields[REQUEST_UID_FIELD] = request_uid

        # Привязка контакта к лиду
        if contact_id:
            lead_fields["CONTACT_ID"] = contact_id

        lr      = bitrix_post("crm.lead.add", {"fields": lead_fields})
        lead_id = lr.get('result') if isinstance(lr, dict) else None
        print(f"{'✅' if lead_id else '⚠️'} Лид: {lead_id or lr}")

        # Проверяем что записалось
        if lead_id:
            check = bitrix_post("crm.lead.get", {"id": lead_id})
            saved = check.get('result', {}).get('COMMENTS', '')
            print(f"🔎 Комментарий лида подтверждён: {bool(saved)}")

        if lead_id:
            return lead_id, "ok", category

        return None, "bitrix_error", category

    except Exception as e:
        print(f"❌ send_to_bitrix exception: {e}")
        return None, "bitrix_error", category

    finally:
        release_lock(phone_norm)


# ============================================
# БОТ
# ============================================
bot = telebot.TeleBot(TOKEN, threaded=False)


def set_reaction(chat_id, message_id, emoji="✅"):
    try:
        from telebot import types
        emoji_map = {"✅": "👍", "❌": "👎", "🤔": "🤔"}
        reaction  = types.ReactionTypeEmoji(emoji_map.get(emoji, "👍"))
        bot.set_message_reaction(chat_id, message_id, [reaction], is_big=False)
        print(f"👍 Реакция поставлена")
    except Exception as e:
        print(f"❌ Реакция: {e}")


@bot.message_handler(commands=['start', 'help'])
def cmd_start(msg):
    bot.reply_to(msg, "✅ Бот активен.")


@bot.message_handler(commands=['myid'])
def cmd_myid(msg):
    if getattr(msg.chat, 'type', None) == 'private':
        bot.reply_to(msg, f"Ваш Telegram ID: {msg.from_user.id}")


def send_review_notifications():
    """Notify one verified admin; failed Telegram DMs remain retryable."""
    if REVIEW_ADMIN_CHAT_ID is None:
        return
    from telebot import types
    with _queue_mutex:
        for review_id, chat_title, body in get_delivery_queue().reviews_to_notify():
            keyboard = types.InlineKeyboardMarkup()
            keyboard.row(
                types.InlineKeyboardButton('✅ Подтвердить',
                                           callback_data=f'rv:a:{review_id}'),
                types.InlineKeyboardButton('❌ Отклонить',
                                           callback_data=f'rv:r:{review_id}'),
            )
            short_body = body if len(body) <= 3000 else body[:3000] + '\n…'
            message = (f'🤔 Возможно, новая заявка (проверка №{review_id})\n'
                       f'Группа: {chat_title}\n\n{short_body}')
            try:
                bot.send_message(REVIEW_ADMIN_CHAT_ID, message,
                                 reply_markup=keyboard)
                get_delivery_queue().review_notified(review_id)
                print(f'✅ Отправлена проверка заявки №{review_id}')
            except Exception as exc:
                get_delivery_queue().review_notify_failed(review_id)
                print(f'⚠️ Не удалось отправить проверку №{review_id}: '
                      f'{type(exc).__name__}')


@bot.callback_query_handler(func=lambda call: bool(getattr(call, 'data', '')
                                                   and call.data.startswith('rv:')))
def handle_review_decision(call):
    """Use only authenticated private callbacks; never trust callback payloads."""
    def answer(message: str):
        try:
            bot.answer_callback_query(call.id, message)
        except Exception:
            pass

    if (REVIEW_ADMIN_CHAT_ID is None
            or getattr(call.from_user, 'id', None) != REVIEW_ADMIN_CHAT_ID
            or getattr(getattr(call.message, 'chat', None), 'id', None)
            != REVIEW_ADMIN_CHAT_ID):
        answer('Нет доступа')
        return
    parts = (call.data or '').split(':')
    if len(parts) != 3 or parts[0] != 'rv' or parts[1] not in ('a', 'r') \
            or not parts[2].isdigit():
        answer('Некорректная команда')
        return

    approve = parts[1] == 'a'
    with _queue_mutex:
        changed, row = get_delivery_queue().decide_review(
            int(parts[2]), approve, REVIEW_ADMIN_CHAT_ID)
        if not changed:
            answer('Решение уже принято или заявка не найдена')
            return
        try:
            bot.edit_message_reply_markup(call.message.chat.id,
                                          call.message.message_id,
                                          reply_markup=None)
        except Exception:
            pass
        if not approve:
            answer('Заявка отклонена')
            return
        answer('Подтверждено: отправляем в CRM')
        chat_id, message_id, chat_title, body, _ = row
        try:
            success = process_queued_message(chat_id, message_id,
                                             chat_title, body)
        except Exception as exc:
            print(f'⚠️ Ошибка отправки подтверждённой заявки: '
                  f'{type(exc).__name__}')
            success = False
        try:
            bot.send_message(REVIEW_ADMIN_CHAT_ID,
                             ('✅ Подтверждённая заявка обработана в CRM.' if success
                              else '⚠️ Заявка сохранена в очереди CRM и будет '
                                   'отправлена повторно автоматически.'))
        except Exception:
            pass


@bot.message_handler(commands=['test'])
def cmd_test(msg):
    if not ADMIN_USER_IDS or getattr(msg.from_user, 'id', None) not in ADMIN_USER_IDS:
        return
    r = bitrix_post("crm.lead.list", {"filter": {"ID": "1"}, "select": ["ID"]})
    if isinstance(r, dict) and 'result' in r:
        bot.reply_to(msg, "✅ Битрикс доступен!")
    else:
        bot.reply_to(msg, f"❌ Ошибка: {r}")


@bot.message_handler(commands=['cache'])
def cmd_cache(msg):
    if not ADMIN_USER_IDS or getattr(msg.from_user, 'id', None) not in ADMIN_USER_IDS:
        return
    with _known_phones_mutex:
        phones = sorted(_known_phones)
    text = f"📋 Кэш ({len(phones)}):\n" + "\n".join(phones[-20:]) if phones else "📋 Кэш пуст"
    bot.reply_to(msg, text)


@bot.message_handler(commands=['sources'])
def cmd_sources(msg):
    if not ADMIN_USER_IDS or getattr(msg.from_user, 'id', None) not in ADMIN_USER_IDS:
        return
    r = bitrix_post("crm.status.list", {"filter": {"ENTITY_ID": "SOURCE"}})
    items = r.get("result", [])
    text  = "📋 Источники в Битрикс:\n\n"
    for item in items:
        text += f"ID: `{item['STATUS_ID']}` → {item['NAME']}\n"
    bot.reply_to(msg, text)


@bot.message_handler(func=lambda m: True, content_types=['text'])
def handle_message(message):
    text = message.text or ""
    if not text or text.startswith('/') or len(text) < 10:
        return
    if getattr(message.from_user, 'is_bot', False):
        return
    # A valid phone is sufficient: contractors often abbreviate the service
    # (e.g. "с/у плиточные работы") or omit repair keywords altogether.
    decision = classify_message(text)
    if decision.kind != 'new_request':
        if needs_manual_review(text, decision):
            chat_title = getattr(message.chat, 'title', None) or 'Личные'
            with _queue_mutex:
                get_delivery_queue().add_review(message.chat.id,
                                                message.message_id,
                                                chat_title, text)
                send_review_notifications()
            return
        print(f'ℹ️ Пропущено сообщение: {decision.kind}/{decision.reason} '
              f'chat={message.chat.id} msg={message.message_id}')
        return

    chat_title = getattr(message.chat, 'title', None) or 'Личные'
    # Journal before Bitrix writes: a failure must not silently consume the update.
    with _queue_mutex:
        get_delivery_queue().enqueue(message.chat.id, message.message_id, chat_title, text)
        process_queued_message(message.chat.id, message.message_id, chat_title, text)


def process_queued_message(chat_id: int, message_id: int, chat_title: str,
                           text: str) -> bool:
    """Return True only after every phone in the Telegram event is handled."""
    if (classify_message(text).kind != 'new_request'
            and not get_delivery_queue().is_review_approved(chat_id, message_id)):
        get_delivery_queue().complete(chat_id, message_id)
        return True

    source_id, source_name = get_source_from_message(chat_title, text)

    print(f"\n{'='*50}")
    print(f"📨 Чат: '{chat_title}' | Источник: {source_name} [{source_id}]")
    print(f'📝 Новая заявка: chat={chat_id} msg={message_id}')

    leads = parse_lead_ai(text) if USE_AI_PARSER and GROQ_API_KEY else []
    actual_phones = set(extract_phones(text))
    leads = [lead for lead in leads
             if normalize_phone(lead.get('phone', '')) in actual_phones]
    if not leads:
        print('ℹ️ Используем локальный парсер заявки')
        leads = parse_lead_fallback(text)

    ok = dup = skip = 0

    for lead in leads:
        lead['request_uid'] = event_uid(chat_id, message_id,
                                        normalize_phone(lead.get('phone', '')))
        lid, status, cat = send_to_bitrix(lead, source_id, source_name, chat_title)
        n = lead.get('name', '—')

        if status == "ok":
            ok += 1
            print(f'✅ Создан лид ID:{lid}')
        elif status == "duplicate":
            dup += 1
            print('⛔ Заявка распознана как дубль')
        elif status == "no_phone":
            skip += 1
            print(f"⚠️ Нет телефона: {n}")
        else:
            print('❌ Не удалось доставить заявку в CRM')

    all_handled = bool(leads) and ok + dup == len(leads)
    if all_handled:
        get_delivery_queue().complete(chat_id, message_id)
        if ok:
            set_reaction(chat_id, message_id, "✅")
        elif dup:
            set_reaction(chat_id, message_id, "❌")
    else:
        get_delivery_queue().defer(chat_id, message_id)
        print(f'⚠️ Заявка ожидает повтора: chat={chat_id} msg={message_id}')
    return all_handled


def retry_pending():
    """Try due events, including those journaled before the last restart."""
    with _queue_mutex:
        for chat_id, message_id, chat_title, text in get_delivery_queue().ready():
            try:
                process_queued_message(chat_id, message_id, chat_title, text)
            except Exception as exc:
                print(f'⚠️ Повтор заявки отложен: {type(exc).__name__}')
                get_delivery_queue().defer(chat_id, message_id)


def retry_loop():
    while True:
        try:
            retry_pending()
            send_review_notifications()
        except Exception as exc:
            # Do not permanently stop retries on an incidental SQLite/CRM error.
            print(f'⚠️ Цикл повторов: {type(exc).__name__}')
        time.sleep(20)


# ============================================
# ЗАПУСК
# ============================================
if __name__ == "__main__":
    print("✅ Бот стартует...")
    if REVIEW_ADMIN_CHAT_ID is None:
        print('⚠️ Ручная проверка: задайте REVIEW_ADMIN_CHAT_ID '
              'или один ADMIN_USER_IDS; сомнительные заявки сохраняются.')
    threading.Thread(target=retry_loop, daemon=True).start()
    try:
        # Preserve updates received while the service was offline.
        bot.delete_webhook(drop_pending_updates=False)
        print("✅ Вебхук удалён")
    except Exception as e:
        print(f"⚠️ Вебхук: {e}")

    time.sleep(3)
    print("🚀 Polling!")

    while True:
        try:
            bot.polling(
                none_stop=True,
                interval=2,
                timeout=30,
                allowed_updates=["message"]
            )
        except Exception as e:
            print(f"❌ Polling: {e}")
            time.sleep(30 if "409" in str(e) else 10)
