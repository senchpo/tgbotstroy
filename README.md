# Telegram → Bitrix24

The bot listens to ordinary text messages in Telegram groups and creates a
Bitrix24 contact and lead for new repair requests.

## Filtering

A message is eligible only when **both** conditions hold:

1. It contains a Russian phone number (starting with `8`, `7`, or `+7`).
2. It contains a repair/service keyword or the word `заявка`.

Known status phrases such as `перезвон`, `недозвон`, `выезд состоялся`, and
`смета отправлена` exclude a message from automatic lead creation. This is a
conservative rule, not a perfect intent detector. Use explicit `заявка` on
unusual requests and review logs when a message is skipped.

Short example accepted:

`89775179200 Борис. М Крылатское с/у плиточные работы. Оп Любовь`

For groups with no source mapped by title, an explicit `Оп Любовь` marker
selects the existing `Трафик Любовь` source. Recognized group titles take
precedence over markers in message text.

## AI

AI is **off by default**; ordinary messages are parsed locally and the
original text is copied into the lead comment. An opt-in `USE_AI_PARSER=true`
plus `GROQ_API_KEY` enables Groq for additional field extraction; if parsing
fails, the local parser remains the fallback.

## Checks before deployment

Run offline tests without a Telegram token or Bitrix24 credentials:

```bash
python -m unittest discover -s tests -v
```

The integration still requires a live staging smoke test before production.
Known limitations: in-memory duplicate cache is not persistent, the bot has no
durable processing queue, and Bitrix contact creation may succeed even if lead
creation fails. Real CRM credentials and production messages are not used by
the offline tests. No Railway deployment is made as part of these changes.
