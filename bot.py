import telebot
import requests
import time
import os
import re
import threading
from datetime import datetime, timedelta

# ============================================
# КОНФИГ
# ============================================
TOKEN        = os.environ.get("TOKEN", "")
BITRIX_URL   = os.environ.get("BITRIX_URL", "")  # https://xxx.bitrix24.ru/rest/1/TOKEN/
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

# ============================================
# ПОТОКОБЕЗОПАСНЫЕ БЛОКИРОВКИ ПО ТЕЛЕФОНУ
# ============================================
_phone_locks: dict[str, datetime] = {}
_locks_mutex = threading.Lock()
LOCK_TTL = 60  # секунд


def acquire_lock(phone: str) -> bool:
    with _locks_mutex:
        now = datetime.now()
        # Чистим устаревшие
        for p in [p for p, t in _phone_locks.items()
                  if (now - t).seconds > LOCK_TTL]:
            del _phone_locks[p]

        if phone in _phone_locks:
            age = (now - _phone_locks[phone]).total_seconds()
            print(f"🔒 Уже обрабатывается ({age:.0f}с): {phone}")
            return False

        _phone_locks[phone] = now
        print(f"🔓 Захвачена блокировка: {phone}")
        return True


def release_lock(phone: str):
    with _locks_mutex:
        _phone_locks.pop(phone, None)
        print(f"🔓 Снята блокировка: {phone}")


# ============================================
# ЛОКАЛЬНЫЙ КЭШ ТЕЛЕФОНОВ (быстрая защита)
# ============================================
_known_phones: set[str] = set()
_known_phones_mutex = threading.Lock()


def is_phone_known_locally(phone: str) -> bool:
    with _known_phones_mutex:
        return phone in _known_phones


def mark_phone_known(phone: str):
    with _known_phones_mutex:
        _known_phones.add(phone)
        print(f"📝 Телефон добавлен в локальный кэш: {phone}")


# ============================================
# КАРТЫ
# ============================================
CHAT_SOURCE_MAP = {
    'авито':        'Реклама Авито',
    'прогмат':      'Прогматремонт(Тимур)',
    'планремонт':   'Планремонта(Денис)',
    'ремонкаждому': 'РемонКаждому(Марина)',
    'каждому':      'РемонКаждому(Марина)',
    'партнерка':    'РемонКаждому(Марина)',
    'реклама':      'Реклама(Владимир)',
    'рбт':          'РБТ(Сергей)',
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
]

# Кэш обработанных сообщений
_processed_msgs: set[str] = set()
_processed_msgs_mutex = threading.Lock()


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


def get_source_from_chat(chat_title: str) -> str:
    if not chat_title:
        return 'Telegram'
    chat_lower = chat_title.lower().replace(' ', '')
    for kw, src in CHAT_SOURCE_MAP.items():
        if kw.lower() in chat_lower:
            return src
    return f'Telegram: {chat_title}'


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
    """Универсальный POST в Битрикс с логированием"""
    url = BITRIX_URL + method
    try:
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json()
        if 'error' in data:
            print(f"⚠️ Битрикс ошибка [{method}]: {data['error']} — {data.get('error_description')}")
        return data
    except Exception as e:
        print(f"❌ Ошибка запроса [{method}]: {e}")
        return {}


# ============================================
# ПРОВЕРКА ДУБЛЕЙ — ГЛАВНАЯ ФУНКЦИЯ
# ============================================
def check_duplicate(phone: str) -> bool:
    """
    Тройная проверка дублей:
    1. Локальный кэш (мгновенно)
    2. crm.duplicate.findByComm — ПРАВИЛЬНЫЙ метод Битрикс
    3. crm.contact.list — резервная
    """

    # 1️⃣ Локальный кэш
    if is_phone_known_locally(phone):
        print(f"⛔ [КЭШ] Телефон уже известен: {phone}")
        return True

    print(f"🔍 Проверяем дубль в Битрикс: {phone}")

    # Убираем + для метода findByComm
    phone_digits = re.sub(r'\D', '', phone)

    # 2️⃣ crm.duplicate.findByComm — ищем в CONTACT
    resp = bitrix_post("crm.duplicate.findByComm", {
        "type": "PHONE",
        "values": [phone_digits],
        "entity_type": "CONTACT"
    })
    result = resp.get("result", {})
    print(f"  findByComm CONTACT: {result}")
    if result.get("CONTACT"):
        print(f"⛔ Найден дубль контакта: {result['CONTACT']}")
        mark_phone_known(phone)
        return True

    # 3️⃣ crm.duplicate.findByComm — ищем в LEAD
    resp2 = bitrix_post("crm.duplicate.findByComm", {
        "type": "PHONE",
        "values": [phone_digits],
        "entity_type": "LEAD"
    })
    result2 = resp2.get("result", {})
    print(f"  findByComm LEAD: {result2}")
    if result2.get("LEAD"):
        print(f"⛔ Найден дубль лида: {result2['LEAD']}")
        mark_phone_known(phone)
        return True

    # 4️⃣ crm.duplicate.findByComm — ищем в DEAL (через contact)
    # Дополнительно проверяем сделки через контакт
    resp3 = bitrix_post("crm.duplicate.findByComm", {
        "type": "PHONE",
        "values": [phone_digits],
        "entity_type": "COMPANY"
    })
    result3 = resp3.get("result", {})
    print(f"  findByComm COMPANY: {result3}")
    if result3.get("COMPANY"):
        print(f"⛔ Найден дубль компании: {result3['COMPANY']}")
        mark_phone_known(phone)
        return True

    print(f"✅ Дублей нет: {phone}")
    return False


# ============================================
# AI ПАРСИНГ
# ============================================
def parse_lead_ai(text: str) -> list[dict]:
    try:
        categories_list = "\n".join([f"- {k}" for k in REPAIR_TYPE_MAP.keys()])

        payload = {
            "model": "llama-3.3-70b-versatile",
            "temperature": 0,
            "messages": [{
                "role": "user",
                "content": f"""Ты парсер заявок на ремонт квартир.
Найди всех клиентов в тексте и верни СТРОГО в формате ниже.

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
- ТЕЛЕФОН: только в формате +7XXXXXXXXXX
- Если данных нет — пиши: Не указано
- Несколько клиентов — ЛИД 1, ЛИД 2 и т.д.

КАТЕГОРИЯ — выбери ОДНУ из списка:
{categories_list}

Правила выбора КАТЕГОРИИ:
- санузел/ванна/туалет → сан узла
- кухня/столешница → кухни
- комната/зал/спальня → комнаты
- балкон/лоджия → балкона
- студия → студия
- однушка/1-комн/1к → 1-комнатная
- двушка/2-комн/2к → 2-комнатная
- трёшка/3-комн/3к/4к+ → 3 и более
- дом/коттедж/дача → дома
- офис/помещение/магазин → офис
- капитальный/под ключ/черновая/чистовая → Капитальный
- косметический/частичный/покраска → Косметический

Текст:
{text}"""
            }]
        }

        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30
        )
        rj = resp.json()

        if 'error' in rj:
            print(f"❌ Groq: {rj['error']}")
            return []

        ai_text = rj['choices'][0]['message']['content']
        print(f"🤖 AI:\n{ai_text}")

        leads = []
        cur = {}

        for line in ai_text.split('\n'):
            line = line.strip()
            if not line:
                continue
            if line.startswith('ЛИД'):
                if cur.get('phone') and cur['phone'] != 'Не указано':
                    leads.append(cur)
                cur = {'raw_text': text}
            elif line.startswith('ИМЯ:'):
                cur['name'] = line[4:].strip()
            elif line.startswith('ТЕЛЕФОН:'):
                cur['phone'] = line[8:].strip()
            elif line.startswith('АДРЕС:'):
                cur['address'] = line[6:].strip()
            elif line.startswith('ОБЪЁМ:'):
                cur['work_volume'] = line[6:].strip()
            elif line.startswith('СРОК:'):
                cur['deadline'] = line[5:].strip()
            elif line.startswith('КАТЕГОРИЯ:'):
                cur['category'] = line[10:].strip()
            elif line.startswith('КОММЕНТАРИЙ:'):
                cur['comment'] = line[12:].strip()
            elif line == '---':
                if cur.get('phone') and cur['phone'] != 'Не указано':
                    leads.append(cur)
                cur = {'raw_text': text}

        if cur.get('phone') and cur['phone'] != 'Не указано':
            leads.append(cur)

        # Дедупликация по телефону внутри одного AI-ответа
        seen = set()
        unique = []
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
def send_to_bitrix(data: dict, source_name: str, chat_title: str) -> tuple:
    phone    = data.get('phone', '').strip()
    name     = data.get('name', 'Не указано').strip()
    address  = data.get('address', 'Не указано').strip()
    volume   = data.get('work_volume', 'Не указано').strip()
    deadline = data.get('deadline', 'Не указано').strip()
    comment  = data.get('comment', 'Не указано').strip()
    category = data.get('category', '').strip()
    raw_text = data.get('raw_text', '')

    if not phone or phone == 'Не указано':
        return None, "no_phone", category

    phone_norm = normalize_phone(phone)
    if len(re.sub(r'\D', '', phone_norm)) < 11:
        print(f"⚠️ Некорректный телефон: {phone}")
        return None, "no_phone", category

    # ШАГ 1: Блокировка по телефону
    if not acquire_lock(phone_norm):
        return None, "duplicate", category

    try:
        # ШАГ 2: Пауза (защита от гонки потоков)
        time.sleep(0.3)

        # ШАГ 3: Проверка дублей (тройная)
        if check_duplicate(phone_norm):
            return None, "duplicate", category

        # ШАГ 4: Создаём записи
        type_id = get_type_id(category)
        title   = f"Ремонт | {name} | {address[:40]}"
        comments = (
            f"📢 Источник: {source_name}\n"
            f"💬 Чат: {chat_title}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📐 Объём: {volume}\n"
            f"📅 Срок: {deadline}\n"
            f"🏷️ Тип: {category}\n"
            f"💬 Комментарий: {comment}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📩 Сообщение:\n{raw_text[:800]}"
        )

        # Контакт
        contact_id = None
        cr = bitrix_post("crm.contact.add", {"fields": {
            "NAME":               name,
            "PHONE":              [{"VALUE": phone_norm, "VALUE_TYPE": "WORK"}],
            "ADDRESS":            address,
            "SOURCE_ID":          "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION": source_name,
            "COMMENTS":           f"Источник: {source_name}\nЧат: {chat_title}",
        }})
        contact_id = cr.get('result')
        print(f"{'✅' if contact_id else '⚠️'} Контакт: {contact_id or cr}")

        # Лид
        lead_id = None
        lead_fields = {
            "TITLE":              title,
            "NAME":               name,
            "PHONE":              [{"VALUE": phone_norm, "VALUE_TYPE": "WORK"}],
            "ADDRESS":            address,
            "COMMENTS":           comments,
            "SOURCE_ID":          "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION": source_name,
        }
        if contact_id:
            lead_fields["CONTACT_ID"] = contact_id
        lr = bitrix_post("crm.lead.add", {"fields": lead_fields})
        lead_id = lr.get('result')
        print(f"{'✅' if lead_id else '⚠️'} Лид: {lead_id or lr}")

        # Сделка
        deal_id = None
        deal_fields = {
            "TITLE":                title,
            "COMMENTS":             comments,
            "SOURCE_ID":            "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION":   source_name,
            "UF_CRM_1775766366237": address,
        }
        if type_id:
            deal_fields["TYPE_ID"] = type_id
        if contact_id:
            deal_fields["CONTACT_IDS"] = [contact_id]
        if lead_id:
            deal_fields["LEAD_ID"] = lead_id
        dr = bitrix_post("crm.deal.add", {"fields": deal_fields})
        deal_id = dr.get('result')
        print(f"{'✅' if deal_id else '⚠️'} Сделка: {deal_id or dr}")

        # ШАГ 5: Запоминаем телефон в локальный кэш
        if contact_id or lead_id:
            mark_phone_known(phone_norm)

        if lead_id or deal_id:
            return lead_id or deal_id, "ok", category

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
        reaction = types.ReactionTypeEmoji(emoji_map.get(emoji, "👍"))
        bot.set_message_reaction(chat_id, message_id, [reaction], is_big=False)
    except Exception as e:
        print(f"❌ Реакция: {e}")


@bot.message_handler(commands=['start', 'help'])
def cmd_start(msg):
    bot.reply_to(msg, "✅ Бот активен.")


@bot.message_handler(commands=['test'])
def cmd_test(msg):
    r = bitrix_post("crm.lead.list", {"filter": {"ID": "1"}, "select": ["ID"]})
    if 'result' in r:
        bot.reply_to(msg, "✅ Битрикс доступен!")
    else:
        bot.reply_to(msg, f"❌ Ошибка: {r}")


@bot.message_handler(commands=['cache'])
def cmd_cache(msg):
    """Показать содержимое локального кэша телефонов"""
    with _known_phones_mutex:
        phones = list(_known_phones)
    if phones:
        bot.reply_to(msg, f"📋 В кэше {len(phones)} телефонов:\n" + "\n".join(phones[-20:]))
    else:
        bot.reply_to(msg, "📋 Кэш пуст")


@bot.message_handler(func=lambda m: True, content_types=['text'])
def handle_message(message):
    # Защита от двойной обработки
    msg_key = f"{message.chat.id}_{message.message_id}"
    with _processed_msgs_mutex:
        if msg_key in _processed_msgs:
            return
        _processed_msgs.add(msg_key)
        if len(_processed_msgs) > 2000:
            _processed_msgs.clear()

    text = message.text or ""
    if not text or text.startswith('/') or len(text) < 10:
        return
    if getattr(message.from_user, 'is_bot', False):
        return

    if not any(kw in text.lower() for kw in KEYWORDS):
        return

    chat_title  = getattr(message.chat, 'title', None) or "Личные"
    source_name = get_source_from_chat(chat_title)

    print(f"\n{'='*50}")
    print(f"📨 Чат: '{chat_title}' | Источник: {source_name}")
    print(f"📝 {text[:200]}")

    leads = parse_lead_ai(text)
    if not leads:
        return

    ok = dup = skip = 0

    for lead in leads:
        lid, status, cat = send_to_bitrix(lead, source_name, chat_title)
        n = lead.get('name', '—')
        p = lead.get('phone', '—')

        if status == "ok":
            ok += 1
            print(f"✅ ID:{lid} | {n} | {p} | {cat}")
        elif status == "duplicate":
            dup += 1
            print(f"⛔ Дубль: {n} | {p}")
        elif status == "no_phone":
            skip += 1
            print(f"⚠️ Нет телефона: {n}")

    if ok:
        set_reaction(message.chat.id, message.message_id, "✅")
    elif dup:
        set_reaction(message.chat.id, message.message_id, "❌")
    elif skip:
        set_reaction(message.chat.id, message.message_id, "🤔")


# ============================================
# ЗАПУСК
# ============================================
if __name__ == "__main__":
    print("✅ Бот стартует...")
    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print(f"⚠️ Вебхук: {e}")
    time.sleep(3)
    print("🚀 Polling!")

    while True:
        try:
            bot.polling(none_stop=True, interval=2, timeout=30,
                        allowed_updates=["message"])
        except Exception as e:
            print(f"❌ Polling: {e}")
            time.sleep(30 if "409" in str(e) else 10)
