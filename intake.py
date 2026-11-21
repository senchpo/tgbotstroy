"""Pure, offline Telegram intake classification. No credentials or network access."""

import re
from dataclasses import dataclass


PHONE_RE = re.compile(
    r'(?<!\d)(?:\+7|7|8)[\s().-]*\d{3}[\s().-]*\d{3}'
    r'[\s().-]*\d{2}[\s().-]*\d{2}(?!\d)'
)

REQUEST_WORDS = (
    'ремонт', 'квартир', 'сануз', 'сан уз', 'с/у', 'с.у.', 'ванн',
    'туалет', 'кухн', 'комнат', 'балкон', 'плит', 'отделк', 'покрас',
    'штукатур', 'студи', 'электрик', 'сантехник', 'столешниц',
    'помещен', 'офис', 'под ключ', 'новострой', 'вторичк',
    'заявка', 'замер', 'смета', 'кв.м', 'м2', 'кв ', 'кап ',
    'стен', 'потолк', 'обои', 'обоев', 'поклейк', 'косм ',
    'косметик', 'капитальн',
)

STATUS_PHRASES = (
    'не дозвони', 'недозвон', 'не актуал', 'неактуал',
    'отказ', 'смета отправ', 'выезд состоял', 'замер состоял',
    'договор подпис', 'в работе', 'закрыт', 'оплат', 'сделка',
    'отложен', 'поставил', 'назначили дело',
    'не берёт', 'не берет', 'не отвечает', 'недоступен',
)

# A customer declining the partner's business card is still a repair lead.
# Remove only that incidental refusal before matching CRM status phrases;
# another refusal (e.g. "от ремонта отказался") must still take precedence.
VISIT_CARD_REFUSAL_RE = re.compile(
    r'\b(?:от\s+визитк[а-яё]*\s+отказ[а-яё]*|'
    r'отказ[а-яё]*\s+от\s+визитк[а-яё]*)\b'
)

NEW_REQUEST_PHRASES = (
    'новая заявка', 'новый клиент', 'новое обращение',
    'просит ремонт', 'нужен ремонт', 'нужно сделать', 'хочет ремонт',
)

# Only a small subset of uncertain, phone-bearing messages merits a manual
# decision. Brief chat, number exchanges and call statuses stay out of DMs.
REVIEW_HINTS = (
    'москв', 'мытищ', 'красногор', 'химк', 'балаших', 'пгт',
    'путилково', 'реутов', 'крылатск', 'коньков',
    'адрес', 'улиц', 'проспект', 'шоссе', 'метро', 'район',
    'посел', 'деревн', 'ламинат', 'двер',
    'окна', 'окон', 'укладк', 'объект', 'перегород', 'шпаклев',
    'утеплен', 'квадрат', 'площад', 'мастер', 'материал',
    'срок', 'стоимост', 'расчёт', 'расчет', 'консультац',
)
REVIEW_EXCLUSIONS = (
    'не берёт', 'не берет', 'не отвечает', 'не дозвони',
    'недоступен', 'звонил', 'звонила', 'новый номер',
    'всё хорошо', 'все хорошо', 'привет', 'спасибо',
    'созвон', 'не актуал', 'неактуал',
)

CUSTOMER_INTENT_PHRASES = (
    'просит', 'попросил', 'попросила', 'хочет', 'интересует',
    'интересовалась', 'интересовался', 'нужно', 'надо',
)


@dataclass(frozen=True)
class Decision:
    kind: str  # new_request | status_update | ambiguous | ignore
    reason: str
    phones: tuple[str, ...]
    confidence: int = 0  # Heuristic score, NOT a calibrated probability.


TEMP_NUMBER_RE = re.compile(r'временн[а-яё]*\s+номер', re.IGNORECASE)
AREA_RE = re.compile(r'\b\d{1,4}\s*(?:м2|м²|м\b|кв\.?\s*м)')


def intake_confidence(text: str, request: bool) -> int:
    """Deterministic 0-100 plausibility score; it is not an ML probability."""
    without_phones = PHONE_RE.sub(' ', text)
    without_label = TEMP_NUMBER_RE.sub(' ', without_phones.casefold())
    named = bool(re.search(r'\b[А-ЯЁ][а-яё]{2,}\b',
                           TEMP_NUMBER_RE.sub(' ', without_phones)))
    area = bool(AREA_RE.search(without_label))
    matched_hints = sum(hint in without_label for hint in REVIEW_HINTS)
    temporary = bool(TEMP_NUMBER_RE.search(text.casefold()))
    return min(99, 45 + 40 * request + 12 * named +
               12 * area + min(30, 10 * matched_hints) + 20 * temporary)


def normalize_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits[0] == '8':
        digits = '7' + digits[1:]
    return '+' + digits


def phones_in(text: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(normalize_phone(m.group()) for m in PHONE_RE.finditer(text)))


def classify_message(text: str) -> Decision:
    phones = phones_in(text)
    if not phones:
        return Decision('ignore', 'no_phone', (), 0)
    value = text.casefold()
    request = any(x in value for x in REQUEST_WORDS)
    confidence = intake_confidence(text, request)
    status_text = VISIT_CARD_REFUSAL_RE.sub(' ', value)
    status = any(x in status_text for x in STATUS_PHRASES)
    followup = 'перезвон' in value
    explicit_new = any(x in value for x in NEW_REQUEST_PHRASES)
    if explicit_new and request:
        return Decision('new_request', 'explicit_new_request', phones, 99)
    if status:
        return Decision('status_update', 'known_status_phrase', phones, 5)
    # An initial customer briefing often says "просит ... перезвонить".
    # A standalone "ремонт кухни, перезвонить" is still a status/task.
    if followup and not (request and any(x in value for x in CUSTOMER_INTENT_PHRASES)):
        return Decision('status_update', 'followup_without_new_intent', phones, 15)
    if request:
        return Decision('new_request', 'phone_and_work_intent', phones,
                        max(85, confidence))
    if TEMP_NUMBER_RE.search(value):
        # Partner intake often starts with "Временный номер" and abbreviates
        # the job as "31м, косметика" or only supplies the contact number.
        return Decision('new_request', 'partner_temporary_number', phones,
                        max(85, confidence))
    if AREA_RE.search(PHONE_RE.sub(' ', value)) and re.search(
            r'\b[А-ЯЁ][а-яё]{2,}\b', PHONE_RE.sub(' ', text)):
        return Decision('new_request', 'phone_name_and_area', phones,
                        max(85, confidence))
    # Partners sometimes send only a phone number, without a name or job details.
    # Do not interpret additional conversation as a lead merely because it has a phone.
    remainder = PHONE_RE.sub(' ', text)
    remainder = re.sub(r'[\s.,;:()\[\]{}\-–—/+]+', '', remainder)
    if not remainder:
        return Decision('new_request', 'phone_only', phones, 95)
    if re.fullmatch(r'[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){0,2}[.!\s]*',
                    PHONE_RE.sub(' ', text).strip()):
        return Decision('new_request', 'phone_and_name', phones, 92)
    return Decision('ambiguous', 'phone_without_work_intent', phones, confidence)


def needs_manual_review(text: str, decision: Decision | None = None) -> bool:
    """Escalate plausible unrecognized leads, not ordinary phone chatter."""
    decision = decision or classify_message(text)
    if decision.kind != 'ambiguous' or not decision.phones:
        return False
    if decision.confidence < 60:
        return False
    remainder = PHONE_RE.sub(' ', text)
    lowered = remainder.casefold()
    if any(marker in lowered for marker in REVIEW_EXCLUSIONS):
        return False
    words = re.findall(r'[а-яёa-z]{3,}', lowered)
    if len(words) < 2:
        return False
    return (any(hint in lowered for hint in REVIEW_HINTS)
            or bool(AREA_RE.search(lowered))
            or bool(re.search(r'\b[мг]\s*\.\s*[а-яё]+', lowered)))


def event_uid(chat_id: int, message_id: int, phone: str) -> str:
    """Deterministic ID; distinct from the lead/customer phone identity."""
    import hashlib
    key = f'tg:v1:{chat_id}:{message_id}:{phone}'
    return 'tg_' + hashlib.sha256(key.encode('utf-8')).hexdigest()[:32]
