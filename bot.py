import telebot
import requests
import time
import os
import re
import uuid

# ============================================
# КОНФИГ
# ============================================

TOKEN        = os.environ.get("TOKEN", "")
BITRIX_URL   = os.environ.get("BITRIX_URL", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

# ============================================
# КАРТЫ И КОНСТАНТЫ
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

REPAIR_TYPES_LIST = list(REPAIR_TYPE_MAP.keys())

KEYWORDS = [
    'ремонт', 'кв.м', 'м2', 'квартира', 'квартиру',
    '+7', 'санузел', 'сан узел', 'кухня', 'балкон',
    'студия', 'комната', 'однушка', 'двушка', 'трёшка',
    'коттедж', 'косметический', 'капитальный', 'замер',
    'покраска', 'штукатурка', 'вторичка', 'новостройка',
    'под ключ', 'чистовая', 'черновая', 'офис',
    'столешница', 'витрина', 'помещение', 'магазин',
]

# ✅ Кэш обработанных сообщений и телефонов
processed_messages = set()
processed_phones   = set()

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def normalize_phone(phone: str) -> str:
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    return '+' + digits if digits.startswith('7') else digits


def get_source_from_chat(chat_title):
    if not chat_title:
        return 'Telegram'
    chat_lower = chat_title.lower().replace(' ', '')
    for keyword, source_name in CHAT_SOURCE_MAP.items():
        if keyword.lower() in chat_lower:
            return source_name
    return f'Telegram: {chat_title}'


def get_type_id(category_name):
    if not category_name:
        return None
    cat_lower = category_name.lower().strip()
    for name, status_id in REPAIR_TYPE_MAP.items():
        if name.lower() == cat_lower:
            return status_id
    for name, status_id in REPAIR_TYPE_MAP.items():
        if cat_lower in name.lower() or name.lower() in cat_lower:
            return status_id
    return None

# ============================================
# ПРОВЕРКА ДУБЛЕЙ ЧЕРЕЗ БИТРИКС
# ============================================

def check_duplicate_in_bitrix(phone_normalized: str) -> bool:
    try:
        print(f"🔍 Проверяем дубль для: {phone_normalized}")

        # Метод 1: crm.duplicate.findbycomm
        resp = requests.get(
            BITRIX_URL + "crm.duplicate.findbycomm.json",
            params={
                "type":        "PHONE",
                "values[]":    phone_normalized,
                "entity_type": "CONTACT",
            },
            timeout=10
        )
        data   = resp.json()
        result = data.get('result', {})
        print(f"  findbycomm: {result}")

        if isinstance(result, dict) and (
            result.get('CONTACT') or
            result.get('LEAD') or
            result.get('DEAL')
        ):
            print(f"⛔ Дубль найден (findbycomm): {result}")
            return True

        # Метод 2: поиск по контактам
        c_resp   = requests.post(
            BITRIX_URL + "crm.contact.list.json",
            json={
                "filter": {"PHONE": phone_normalized},
                "select": ["ID", "NAME"],
            },
            timeout=10
        )
        c_result = c_resp.json().get('result', [])
        print(f"  contact.list: {c_result}")
        if c_result:
            print(f"⛔ Дубль в контактах: {c_result}")
            return True

        # Метод 3: поиск по лидам
        l_resp   = requests.post(
            BITRIX_URL + "crm.lead.list.json",
            json={
                "filter": {"PHONE": phone_normalized},
                "select": ["ID", "NAME"],
            },
            timeout=10
        )
        l_result = l_resp.json().get('result', [])
        print(f"  lead.list: {l_result}")
        if l_result:
            print(f"⛔ Дубль в лидах: {l_result}")
            return True

        # Метод 4: поиск по сделкам
        d_resp   = requests.post(
            BITRIX_URL + "crm.deal.list.json",
            json={
                "filter": {"PHONE": phone_normalized},
                "select": ["ID", "TITLE"],
            },
            timeout=10
        )
        d_result = d_resp.json().get('result', [])
        print(f"  deal.list: {d_result}")
        if d_result:
            print(f"⛔ Дубль в сделках: {d_result}")
            return True

        print(f"✅ Дублей нет для: {phone_normalized}")
        return False

    except Exception as e:
        print(f"❌ Ошибка проверки дублей: {e}")
        return True

# ============================================
# AI ПАРСИНГ
# ============================================

def parse_lead_ai(text):
    try:
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type":  "application/json",
        }

        categories_list = "\n".join([f"- {k}" for k in REPAIR_TYPES_LIST])

        payload = {
            "model":       "llama-3.3-70b-versatile",
            "temperature": 0,
            "messages": [{
                "role": "user",
                "content": f"""Ты парсер заявок на ремонт квартир.
Найди всех клиентов в тексте и верни данные СТРОГО в формате ниже.
Если клиент один — один блок ЛИД 1. Если несколько — ЛИД 1, ЛИД 2 и т.д.

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
- ИМЯ: имя клиента (если нет — Не указано)
- ТЕЛЕФОН: номер телефона клиента в формате +7XXXXXXXXXX
- АДРЕС: город, улица, дом (текстом)
- ОБЪЁМ: метраж и описание работ
- СРОК: когда планирует ремонт
- КОММЕНТАРИЙ: любые важные детали
- Если данных нет — пиши ровно: Не указано

КАТЕГОРИЯ — выбери ОДНУ точную строку из списка ниже:
{categories_list}

Правила выбора КАТЕГОРИИ:
- санузел / ванна / туалет / с/у → сан узла
- кухня / столешница / витрина → кухни
- комната / зал / спальня / гостиная → комнаты
- балкон / лоджия → балкона
- студия / студ → студия
- однушка / 1-комн / 1 комн / 1к → 1-комнатная
- двушка / 2-комн / 2 комн / 2к → 2-комнатная
- трёшка / 3-комн / 3 комн / 3к / 4к и более → 3 и более
- дом / коттедж / таунхаус / дача → дома
- офис / коммерческая / помещение / магазин → офис
- капитальный / евро / под ключ / черновая / чистовая → Капитальный
- косметический / частичный / освежить / покраска → Косметический

Текст заявки:
{text}"""
            }]
        }

        response    = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        result_json = response.json()

        if 'error' in result_json:
            print(f"❌ Groq ошибка: {result_json['error']}")
            return []

        result_text = result_json['choices'][0]['message']['content']
        print(f"AI ответ:\n{result_text}")

        leads        = []
        current_lead = {}

        for line in result_text.split('\n'):
            line = line.strip()
            if not line:
                continue
            if line.startswith('ЛИД'):
                if current_lead and current_lead.get('phone') and current_lead['phone'] != 'Не указано':
                    leads.append(current_lead)
                current_lead = {'raw_text': text}
            elif line.startswith('ИМЯ:'):
                current_lead['name']        = line.replace('ИМЯ:', '').strip()
            elif line.startswith('ТЕЛЕФОН:'):
                current_lead['phone']       = line.replace('ТЕЛЕФОН:', '').strip()
            elif line.startswith('АДРЕС:'):
                current_lead['address']     = line.replace('АДРЕС:', '').strip()
            elif line.startswith('ОБЪЁМ:'):
                current_lead['work_volume'] = line.replace('ОБЪЁМ:', '').strip()
            elif line.startswith('СРОК:'):
                current_lead['deadline']    = line.replace('СРОК:', '').strip()
            elif line.startswith('КАТЕГОРИЯ:'):
                current_lead['category']    = line.replace('КАТЕГОРИЯ:', '').strip()
            elif line.startswith('КОММЕНТАРИЙ:'):
                current_lead['comment']     = line.replace('КОММЕНТАРИЙ:', '').strip()
            elif line == '---':
                if current_lead and current_lead.get('phone') and current_lead['phone'] != 'Не указано':
                    leads.append(current_lead)
                current_lead = {'raw_text': text}

        if current_lead and current_lead.get('phone') and current_lead['phone'] != 'Не указано':
            leads.append(current_lead)

        # ✅ Совет Анны — убираем дубли по телефону внутри одного AI ответа
        seen_phones  = set()
        unique_leads = []
        for lead in leads:
            p = normalize_phone(lead.get('phone', ''))
            if p and p not in seen_phones:
                seen_phones.add(p)
                unique_leads.append(lead)
            else:
                print(f"⚠️ Дубль телефона в AI ответе пропущен: {p}")

        print(f"Найдено лидов: {len(unique_leads)}")
        return unique_leads

    except Exception as e:
        print(f"❌ AI ошибка: {e}")
        return []

# ============================================
# ОТПРАВКА В БИТРИКС
# ============================================

def send_to_bitrix(data, source_name, chat_title):
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

    phone_normalized = normalize_phone(phone)
    digits_only      = re.sub(r'\D', '', phone_normalized)

    if len(digits_only) < 11:
        print(f"⚠️ Некорректный телефон: {phone} → {phone_normalized}")
        return None, "no_phone", category

    # ✅ Проверка локального кэша телефонов
    if phone_normalized in processed_phones:
        print(f"⚠️ Телефон уже обрабатывается в этой сессии: {phone_normalized}")
        return None, "duplicate", category

    # ✅ Проверка дублей в Битриксе
    if check_duplicate_in_bitrix(phone_normalized):
        return None, "duplicate", category

    # ✅ Добавляем в локальный кэш СРАЗУ после проверки
    processed_phones.add(phone_normalized)
    if len(processed_phones) > 500:
        processed_phones.clear()

    # ✅ Совет Анны №3 — уникальный ID заявки
    request_uid = str(uuid.uuid4())
    print(f"🆔 UID заявки: {request_uid}")

    type_id = get_type_id(category)

    comments = (
        f"📢 Источник: {source_name}\n"
        f"💬 Чат: {chat_title}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📐 Объём: {volume}\n"
        f"📅 Срок: {deadline}\n"
        f"🏷️ Тип ремонта: {category}\n"
        f"💬 Комментарий: {comment}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📩 Исходное сообщение:\n{raw_text[:800]}"
    )

    title = f"Ремонт | {name} | {address[:50]}"

    # ✅ Создаём контакт
    contact_id = None
    try:
        contact_resp = requests.post(
            BITRIX_URL + "crm.contact.add.json",
            json={"fields": {
                "NAME":               name,
                "PHONE":              [{"VALUE": phone_normalized, "VALUE_TYPE": "WORK"}],
                "ADDRESS":            address,
                "SOURCE_ID":          "UC_CRM_SOURCE",
                "SOURCE_DESCRIPTION": source_name,
                "COMMENTS":           f"Источник: {source_name}\nЧат: {chat_title}",
            }},
            timeout=10
        )
        resp_data  = contact_resp.json()
        contact_id = resp_data.get('result')
        if contact_id:
            print(f"✅ Контакт создан ID: {contact_id}")
        else:
            print(f"⚠️ Контакт не создан: {resp_data}")
    except Exception as e:
        print(f"❌ Ошибка создания контакта: {e}")

    # ✅ Создаём лид
    lead_id = None
    try:
        lead_fields = {
            "TITLE":                title,
            "NAME":                 name,
            "PHONE":                [{"VALUE": phone_normalized, "VALUE_TYPE": "WORK"}],
            "ADDRESS":              address,
            "COMMENTS":             comments,
            "SOURCE_ID":            "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION":   source_name,
            "UF_CRM_REQUEST_UID":   request_uid,  # ✅ уникальный ID
        }
        if contact_id:
            lead_fields["CONTACT_ID"] = contact_id

        lead_resp = requests.post(
            BITRIX_URL + "crm.lead.add.json",
            json={"fields": lead_fields},
            timeout=10
        )
        resp_data = lead_resp.json()
        lead_id   = resp_data.get('result')
        if lead_id:
            print(f"✅ Лид создан ID: {lead_id}")
        else:
            print(f"⚠️ Лид не создан: {resp_data}")
    except Exception as e:
        print(f"❌ Ошибка создания лида: {e}")

    # ✅ Создаём сделку
    deal_id = None
    try:
        deal_fields = {
            "TITLE":                title,
            "COMMENTS":             comments,
            "SOURCE_ID":            "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION":   source_name,
            "UF_CRM_1775766366237": address,
            "UF_CRM_REQUEST_UID":   request_uid,  # ✅ уникальный ID
        }
        if type_id:
            deal_fields["TYPE_ID"] = type_id
        if contact_id:
            deal_fields["CONTACT_IDS"] = [contact_id]
        if lead_id:
            deal_fields["LEAD_ID"] = lead_id

        deal_resp = requests.post(
            BITRIX_URL + "crm.deal.add.json",
            json={"fields": deal_fields},
            timeout=10
        )
        resp_data = deal_resp.json()
        deal_id   = resp_data.get('result')
        if deal_id:
            print(f"✅ Сделка создана ID: {deal_id}")
        else:
            print(f"⚠️ Сделка не создана: {resp_data}")
    except Exception as e:
        print(f"❌ Ошибка создания сделки: {e}")

    if lead_id or deal_id:
        return lead_id or deal_id, "ok", category

    return None, "bitrix_error", category

# ============================================
# БОТ
# ============================================

bot = telebot.TeleBot(TOKEN, threaded=False)


def set_reaction(chat_id, message_id, emoji="✅"):
    try:
        from telebot import types
        VALID_REACTIONS = {
            "✅": "👍",
            "❌": "👎",
            "🤔": "🤔",
        }
        safe_emoji = VALID_REACTIONS.get(emoji, "👍")
        reaction   = types.ReactionTypeEmoji(safe_emoji)
        bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[reaction],
            is_big=False
        )
        print(f"✅ Реакция {safe_emoji} поставлена")
    except Exception as e:
        print(f"❌ Ошибка реакции: {e}")


@bot.message_handler(commands=['start', 'help'])
def cmd_start(message):
    bot.reply_to(message, "✅ Бот активен.")


@bot.message_handler(commands=['test'])
def cmd_test(message):
    try:
        r = requests.get(BITRIX_URL + "crm.lead.list.json", timeout=5)
        if r.json().get('result') is not None:
            bot.reply_to(message, "✅ Связь с Битрикс24 работает!")
        else:
            bot.reply_to(message, f"❌ Ошибка: {r.json()}")
    except Exception as e:
        bot.reply_to(message, f"❌ Нет связи: {e}")


@bot.message_handler(func=lambda m: True, content_types=['text'])
def handle_message(message):

    # ✅ ЗАЩИТА ОТ ДВОЙНОЙ ОБРАБОТКИ — самая первая проверка
    msg_key = f"{message.chat.id}_{message.message_id}"
    if msg_key in processed_messages:
        print(f"⚠️ Сообщение уже обработано, пропускаем: {msg_key}")
        return
    processed_messages.add(msg_key)
    if len(processed_messages) > 1000:
        processed_messages.clear()

    text = message.text
    if not text or text.startswith('/') or len(text) < 10:
        return

    if message.from_user and message.from_user.is_bot:
        return

    text_lower = text.lower()
    is_lead    = any(kw.lower() in text_lower for kw in KEYWORDS)
    if not is_lead:
        return

    chat_title  = getattr(message.chat, 'title', None) or "Личные сообщения"
    source_name = get_source_from_chat(chat_title)

    print(f"\n{'='*50}")
    print(f"📨 Заявка из чата: '{chat_title}'")
    print(f"📢 Источник: {source_name}")
    print(f"📝 Текст: {text[:300]}")
    print(f"{'='*50}")

    leads = parse_lead_ai(text)
    if not leads:
        print("⚠️ Лиды не найдены")
        return

    success_count   = 0
    duplicate_count = 0
    skip_count      = 0

    for lead in leads:
        lead_id, status, category = send_to_bitrix(lead, source_name, chat_title)
        name  = lead.get('name', '—')
        phone = lead.get('phone', '—')

        if status == "ok":
            success_count += 1
            print(f"✅ Готово! ID:{lead_id} | {name} | {phone} | {category}")
        elif status == "duplicate":
            duplicate_count += 1
            print(f"⛔ Дубль: {name} | {phone}")
        elif status == "no_phone":
            skip_count += 1
            print(f"⚠️ Нет телефона: {name}")
        else:
            print(f"❌ Ошибка для: {name} | {phone}")

    if success_count > 0:
        set_reaction(message.chat.id, message.message_id, "✅")
    elif duplicate_count > 0:
        set_reaction(message.chat.id, message.message_id, "❌")
    elif skip_count > 0:
        set_reaction(message.chat.id, message.message_id, "🤔")


# ============================================
# ЗАПУСК
# ============================================

if __name__ == "__main__":
    print("=" * 50)
    print("✅ Бот запускается...")
    print("=" * 50)

    try:
        bot.delete_webhook(drop_pending_updates=True)
        print("✅ Вебхук удалён")
    except Exception as e:
        print(f"⚠️ Ошибка удаления вебхука: {e}")

    print("⏳ Ждём 3 секунды...")
    time.sleep(3)
    print("🚀 Запускаем polling!")

    while True:
        try:
            bot.polling(
                none_stop=True,
                interval=2,
                timeout=30,
                allowed_updates=["message"]
            )
        except Exception as e:
            print(f"❌ Polling упал: {e}")
            if "409" in str(e):
                print("⏳ Конфликт! Ждём 30 секунд...")
                time.sleep(30)
            else:
                print("🔄 Перезапуск через 10 секунд...")
                time.sleep(10)
