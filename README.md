# Telegram → Bitrix24

The bot listens to ordinary text messages in Telegram groups and creates a
Bitrix24 contact and lead for new repair requests.

## Filtering

A message is eligible only when **both** conditions hold:

1. It contains a Russian phone number (starting with `8`, `7`, or `+7`).
2. It contains a repair/service keyword or the word `заявка`.

The `intake.py` classifier returns `new_request`, `status_update`, `ambiguous`,
or `ignore` with a machine-readable reason. Known status phrases such as
`перезвон`, `недозвон`, `выезд состоялся`, and `смета отправлена` exclude
messages unless they clearly indicate a *new* request. This is a conservative
rule, not a perfect intent detector. Ambiguous messages are logged without
creating leads. Use explicit `новая заявка` on unusual requests.

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

For a **read-only metadata audit**, run `python audit_crm_fields.py` in a
trusted environment with the existing `BITRIX_URL` variable. It calls only
`crm.lead.fields`, `crm.contact.fields`, `crm.lead.userfield.list`, and
`crm.status.list`. It prints field/status metadata, never the webhook URL or
customer records. Do not paste the webhook URL into an issue, commit, or log.

The integration still requires a live staging smoke test before production.
An opt-in test harness is available at `tests/test_bitrix_integration.py`.
It refuses any host other than `b24-frslge.bitrix24.ru`, requires the UID
field to exist before writing, creates a single synthetic contact+lead,
verifies their linkage and UID, and deletes only the IDs it created in a
`finally` block. If cleanup fails, it reports the exact IDs for manual
recovery. It is **never** invoked by offline unit tests or normal bot startup.
Run only with `TEST_BITRIX_URL` configured in a trusted test environment.
Known limitations: in-memory duplicate cache is not persistent, the bot has no
durable processing queue, and Bitrix contact creation may succeed even if lead
creation fails. Real CRM credentials and production messages are not used by
the offline tests. No Railway deployment is made as part of these changes.

The field labelled `UF_CRM_REQUEST_UID` was confirmed by a **read-only** REST
audit of the production portal on 2026-11-18 as `UF_CRM_1776153852` (string).
The bot now writes a deterministic Telegram request UID to this field and
checks `crm.lead.list` before creating a lead. Existing contacts can therefore
receive new leads without being blocked by phone-only deduplication.
However, this lookup is not an atomic uniqueness constraint: simultaneous
replicas, a network timeout during creation, or a crash between the lookup and
write can still create duplicates. Exactly-once delivery is **not guaranteed**
until a durable outbox/reconciliation workflow and integration test exist.
The portal also does not expose `TYPE_ID` in `crm.lead.fields`; repair category
remains in `COMMENTS` instead of being sent to an unsupported field.

PII in routine logging has been reduced. Existing Railway historical logs are
not modified or deleted by this patch.
