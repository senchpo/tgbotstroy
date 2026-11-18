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
)

STATUS_PHRASES = (
    'не дозвони', 'недозвон', 'перезвон', 'не актуал', 'неактуал',
    'отказ', 'смета отправ', 'выезд состоял', 'замер состоял',
    'договор подпис', 'в работе', 'закрыт', 'оплат', 'сделка',
    'отложен', 'поставил', 'назначили дело',
)

NEW_REQUEST_PHRASES = (
    'новая заявка', 'новый клиент', 'новое обращение',
    'просит ремонт', 'нужен ремонт', 'нужно сделать', 'хочет ремонт',
)


@dataclass(frozen=True)
class Decision:
    kind: str  # new_request | status_update | ambiguous | ignore
    reason: str
    phones: tuple[str, ...]


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
        return Decision('ignore', 'no_phone', ())
    value = text.casefold()
    request = any(x in value for x in REQUEST_WORDS)
    status = any(x in value for x in STATUS_PHRASES)
    explicit_new = any(x in value for x in NEW_REQUEST_PHRASES)
    if explicit_new and request:
        return Decision('new_request', 'explicit_new_request', phones)
    if status:
        return Decision('status_update', 'known_status_phrase', phones)
    if request:
        return Decision('new_request', 'phone_and_work_intent', phones)
    return Decision('ambiguous', 'phone_without_work_intent', phones)


def event_uid(chat_id: int, message_id: int, phone: str) -> str:
    """Deterministic ID; distinct from the lead/customer phone identity."""
    import hashlib
    key = f'tg:v1:{chat_id}:{message_id}:{phone}'
    return 'tg_' + hashlib.sha256(key.encode('utf-8')).hexdigest()[:32]
