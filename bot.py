import telebot
import requests
import time
import os

# ============================================
# НАСТРОЙКИ — берём из переменных окружения
# ============================================

TOKEN        = os.environ.get("TOKEN", "")
BITRIX_URL   = os.environ.get("BITRIX_URL", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

# ============================================
# ИСТОЧНИКИ ЧАТОВ → БИТРИКС24
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

def get_source_from_chat(chat_title):
    if not chat_title:
        return 'Telegram'
    chat_lower = chat_title.lower().replace(' ', '')
    for keyword, source_name in CHAT_SOURCE_MAP.items():
        if keyword.lower() in chat_lower:
            print(f"✅ Чат '{chat_title}' → {source_name}")
            return source_name
    return f'Telegram: {chat_title}'

# ============================================
# ТИП СДЕЛКИ → STATUS_ID ИЗ БИТРИКС24
# ============================================

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

def get_type_id(category_name):
    if not category_name:
        return None
    cat_lower = category_name.lower().strip()
    for name, status_id in REPAIR_TYPE_MAP.items():
        if name.lower() == cat_lower:
            print(f"🏷️ Категория точно: '{name}' → {status_id}")
            return status_id
    for name, status_id in REPAIR_TYPE_MAP.items():
        if cat_lower in name.lower() or name.lower() in cat_lower:
            print(f"🏷️ Категория приблизительно: '{name}' → {status_id}")
            return status_id
    print(f"⚠️ Категория не найдена: '{category_name}'")
    return None

# ============================================
# КЛЮЧЕВЫЕ СЛОВА
# ============================================

KEYWORDS = [
    'ремонт', 'кв.м', 'м2', 'квартира', 'квартиру',
    '+7', 'санузел', 'сан узел', 'кухня', 'балкон',
    'студия', 'комната', 'однушка', 'двушка', 'трёшка',
    'коттедж', 'косметический', 'капитальный', 'замер',
    'покраска', 'штукатурка', 'вторичка', 'новостройка',
    'под ключ', 'чистовая', 'черновая', 'офис',
]

# ============================================
# AI ПАРСИНГ ЧЕРЕЗ GROQ
# ============================================

def parse_lead_ai(text):
    try:
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type":  "application/json"
        }

        categories_list = "\n".join([f"- {k}" for k in REPAIR_TYPES_LIST])

        payload = {
            "model": "llama-3.3-70b-versatile",
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

КАТЕГОРИЯ — выбери ОДНУ точную строку из списка ниже (скопируй точно как написано):
{categories_list}

Правила выбора КАТЕГОРИИ:
- санузел / ванна / туалет / с/у → сан узла
- кухня → кухни
- комната / зал / спальня / гостиная → комнаты
- балкон / лоджия → балкона
- студия / студ → студия
- однушка / 1-комн / 1 комн / 1к → 1-комнатная
- двушка / 2-комн / 2 комн / 2к → 2-комнатная
- трёшка / 3-комн / 3 комн / 3к / 4к и более → 3 и более
- дом / коттедж / таунхаус / дача → дома
- офис / коммерческая → офис
- капитальный / евро / под ключ / черновая / чистовая → Капитальный
- косметический / частичный / освежить → Косметический
- если тип не понятен — выбери ближайшую по смыслу

Текст заявки:
{text}"""
            }]
        }

        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )

        print(f"Groq статус: {response.status_code}")
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
                current_lead['name'] = line.replace('ИМЯ:', '').strip()

            elif line.startswith('ТЕЛЕФОН:'):
                current_lead['phone'] = line.replace('ТЕЛЕФОН:', '').strip()

            elif line.startswith('АДРЕС:'):
                current_lead['address'] = line.replace('АДРЕС:', '').strip()

            elif line.startswith('ОБЪЁМ:'):
                current_lead['work_volume'] = line.replace('ОБЪЁМ:', '').strip()

            elif line.startswith('СРОК:'):
                current_lead['deadline'] = line.replace('СРОК:', '').strip()

            elif line.startswith('КАТЕГОРИЯ:'):
                current_lead['category'] = line.replace('КАТЕГОРИЯ:', '').strip()

            elif line.startswith('КОММЕНТАРИЙ:'):
                current_lead['comment'] = line.replace('КОММЕНТАРИЙ:', '').strip()

            elif line == '---':
                if current_lead and current_lead.get('phone') and current_lead['phone'] != 'Не указано':
                    leads.append(current_lead)
                current_lead = {'raw_text': text}

        if current_lead and current_lead.get('phone') and current_lead['phone'] != 'Не указано':
            leads.append(current_lead)

        print(f"Найдено лидов: {len(leads)}")
        return leads

    except Exception as e:
        print(f"❌ AI ошибка: {e}")
        return []

# ============================================
# ОТПРАВКА В БИТРИКС24
# ============================================

def send_to_bitrix(data, source_name, chat_title):
    try:
        phone    = data.get('phone', '').strip()
        name     = data.get('name', 'Не указано').strip()
        address  = data.get('address', 'Не указано').strip()
        volume   = data.get('work_volume', 'Не указано').strip()
        deadline = data.get('deadline', 'Не указано').strip()
        comment  = data.get('comment', 'Не указано').strip()
        category = data.get('category', '').strip()
        raw_text = data.get('raw_text', '')

        if not phone or phone == 'Не указано':
            print(f"⚠️ Нет телефона у {name}")
            return None, "no_phone", category

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

        # ШАГ 1: Создаём контакт
        contact_fields = {
            "NAME":               name,
            "PHONE":              [{"VALUE": phone, "VALUE_TYPE": "WORK"}],
            "ADDRESS":            address,
            "SOURCE_ID":          "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION": source_name,
            "COMMENTS":           f"Источник: {source_name}\nЧат: {chat_title}",
        }

        contact_resp = requests.post(
            BITRIX_URL + "crm.contact.add.json",
            json={"fields": contact_fields},
            timeout=10
        )
        contact_id = contact_resp.json().get('result')
        print(f"Контакт создан ID: {contact_id}")

        # ШАГ 2: Создаём лид
        lead_fields = {
            "TITLE":              title,
            "NAME":               name,
            "PHONE":              [{"VALUE": phone, "VALUE_TYPE": "WORK"}],
            "ADDRESS":            address,
            "COMMENTS":           comments,
            "SOURCE_ID":          "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION": source_name,
        }

        if contact_id:
            lead_fields["CONTACT_ID"] = contact_id

        lead_resp   = requests.post(
            BITRIX_URL + "crm.lead.add.json",
            json={"fields": lead_fields},
            timeout=10
        )
        lead_result = lead_resp.json()
        lead_id     = lead_result.get('result')
        print(f"Лид создан ID: {lead_id} | Ответ: {lead_result}")

        # ШАГ 3: Создаём сделку
        deal_fields = {
            "TITLE":                title,
            "COMMENTS":             comments,
            "SOURCE_ID":            "UC_CRM_SOURCE",
            "SOURCE_DESCRIPTION":   source_name,
            "UF_CRM_1775766366237": address,
        }

        if type_id:
            deal_fields["TYPE_ID"] = type_id
            print(f"✅ TYPE_ID установлен: {type_id} ({category})")

        if contact_id:
            deal_fields["CONTACT_IDS"] = [contact_id]

        if lead_id:
            deal_fields["LEAD_ID"] = lead_id

        deal_resp   = requests.post(
            BITRIX_URL + "crm.deal.add.json",
            json={"fields": deal_fields},
            timeout=10
        )
        deal_result = deal_resp.json()
        deal_id     = deal_result.get('result')
        print(f"Сделка создана ID: {deal_id} | Ответ: {deal_result}")

        if lead_id or deal_id:
            return lead_id or deal_id, "ok", category

        return None, "bitrix_error", category

    except Exception as e:
        print(f"❌ Битрикс ошибка: {e}")
        return None, "error", category

# ============================================
# ИНИЦИАЛИЗАЦИЯ
# ============================================

bot = telebot.TeleBot(TOKEN)
bot.remove_webhook()
time.sleep(1)

# ============================================
# ЗАЩИТА ОТ ДУБЛЕЙ
# ============================================

processed_messages = set()

# ============================================
# КОМАНДЫ
# ============================================

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

# ============================================
# ОБРАБОТКА СООБЩЕНИЙ
# ============================================

@bot.message_handler(func=lambda m: True, content_types=['text'])
def handle_message(message):

    # Защита от дублей
    msg_id = message.message_id
    if msg_id in processed_messages:
        print(f"⚠️ Сообщение {msg_id} уже обработано — пропускаем")
        return
    processed_messages.add(msg_id)
    if len(processed_messages) > 1000:
        processed_messages.clear()

    text = message.text
    if not text or text.startswith('/') or len(text) < 10:
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

    success_count = 0
    report_lines  = []

    for lead in leads:
        lead_id, status, category = send_to_bitrix(lead, source_name, chat_title)

        name     = lead.get('name', '—')
        phone    = lead.get('phone', '—')
        address  = lead.get('address', 'Не указано')
        volume   = lead.get('work_volume', 'Не указано')
        deadline = lead.get('deadline', 'Не указано')

        if status == "ok":
            success_count += 1
            print(f"✅ Готово! ID:{lead_id} | {name} | {phone} | {category}")
            report_lines.append(
                f"━━━━━━━━━━━━━━━\n"
                f"🆔 Лид #{lead_id}\n"
                f"👤 {name}\n"
                f"📞 {phone}\n"
                f"📍 {address}\n"
                f"📐 {volume}\n"
                f"📅 {deadline}\n"
                f"🏷️ {category}\n"
            )

        elif status == "no_phone":
            print(f"⚠️ Пропущен (нет телефона): {name}")
            report_lines.append(
                f"━━━━━━━━━━━━━━━\n"
                f"⚠️ Пропущен (нет телефона)\n"
                f"👤 {name}"
            )

        else:
            print(f"❌ Ошибка для: {name}")
            report_lines.append(
                f"━━━━━━━━━━━━━━━\n"
                f"❌ Ошибка создания лида\n"
                f"👤 {name} | 📞 {phone}"
            )
                    header      = f"✅ Создано лидов: {success_count}\n\n"
        full_report = header + "\n\n".join(report_lines)
        try:
            bot.send_message(message.chat.id, full_report)
        except Exception as e:
            print(f"❌ Ошибка отправки сообщения: {e}")

# ============================================
# ЗАПУСК
# ============================================

print("=" * 50)
print("✅ Бот запущен!")
print("=" * 50)

bot.remove_webhook()
time.sleep(3)

bot.polling(
    none_stop=True,
    interval=1,
    timeout=30,
    allowed_updates=["message"]
)
