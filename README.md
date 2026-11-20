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

### Reliable delivery and Railway configuration

New requests are journaled in a SQLite `pending` table *before* attempting
CRM writes. Transient failures are retried with bounded exponential backoff
(10 seconds to 1 hour); successful events are removed. On restart, the retry
worker resumes pending events. Each Telegram event/phone has a deterministic
UID, so a successful partial delivery will be recognized on replay.

**Important:** configure `QUEUE_DB_PATH` on a **persistent Railway volume**,
e.g. `/data/telegram_pending.sqlite3`. The default relative path is suitable
for local testing but may disappear on Railway redeploys. A single bot replica
is assumed: SQLite and the CRM UID lookup are not a distributed lock. Ensure
only one polling process owns the Telegram bot token.

Set `ADMIN_USER_IDS` to comma-separated Telegram numeric user IDs for the
diagnostic `/test`, `/cache`, `/sources` commands. Without this setting those
commands are disabled. Keep `USE_AI_PARSER=false` unless third-party AI data
processing is explicitly intended.

### Manual review of uncertain Telegram requests

Clear repair requests (including phone-only messages) go to Bitrix24 without
manual approval. Obvious phone-bearing statuses and ordinary chat are ignored.
Only *plausible but unrecognized* phone-bearing requests are stored in a
`manual_review` SQLite table, and the designated admin receives a private
Telegram message with **Confirm** / **Reject** inline buttons. The table is on
the same persistent `QUEUE_DB_PATH` volume as the delivery queue.

- Set `REVIEW_ADMIN_CHAT_ID` to **one trusted person's Telegram numeric ID**;
  this user receives customer names, phone numbers, and raw chat text.
  Alternatively, if `ADMIN_USER_IDS` contains exactly one ID, that ID is used.
- The admin must send `/start` to the bot in a private Telegram chat before
  Telegram permits the bot to send private notifications. `/myid` in that chat
  returns the admin's numeric ID for configuration.
- Without either configuration, doubtful requests **remain stored** but no
  private notification is sent. Once the ID is set and the bot restarts, pending
  notifications are retried automatically.
- Confirming atomically moves a message to the normal Bitrix retry queue, and
  the existing Telegram UID deduplication applies. If Bitrix is unavailable,
  the confirmation stays in the persistent queue and retries automatically.
  Rejecting keeps only review metadata, without the original text. Repeated
  button presses never create additional CRM leads.
- Failed private notifications retry with backoff. Very rarely, a crash after
  Telegram accepts a notification but before its receipt is journaled can
  create two notification messages. Only the first confirmation is applied.
- The review classifier intentionally minimizes interruptions; it cannot
  detect every possible uncommon lead wording. Admins should periodically
  reconcile Telegram messages with CRM to find remaining misses.

Never set the review recipient to a shared group or an unverified Telegram ID.
The review table may retain personal data until a decision is made; protect
and back up the Railway volume accordingly.

The pending queue contains original Telegram messages, including names and
phone numbers, **until delivery succeeds**. Protect the volume with Railway
access controls and backups. Never commit the SQLite file.

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
Known limitations: the SQLite queue is durable only if backed by a mounted
volume; Bitrix contact creation may succeed even if lead creation fails. Real
CRM credentials and production messages are not used by
the offline tests. A GitHub push can trigger an automatic Railway deployment
when that branch is linked to the service; verify the live deployment
separately before relying on new functionality.

The field labelled `UF_CRM_REQUEST_UID` was confirmed by a **read-only** REST
audit of the production portal on 2026-11-18 as `UF_CRM_1776153852` (string).
The bot now writes a deterministic Telegram request UID to this field and
checks `crm.lead.list` before creating a lead. **A different Telegram message
is not a new customer:** before creating a contact or lead, the bot also calls
`crm.duplicate.findByComm` for both CONTACT and LEAD using the normalized phone.
If either exists in Bitrix24 (regardless of the partner, name, lead stage or
work description), the new Telegram request is discarded as a duplicate. This
is deliberately strict: partners reusing temporary phone numbers will have
legitimate new requests rejected until the policy is changed.

After an event passes its initial phone check, a `phone_prechecks` SQLite
journal entry is committed **before** any CRM write. If contact creation works
but lead creation fails, retrying the same Telegram event may reuse that
contact without being blocked as a duplicate of itself. A separate Telegram
event with the same phone is still rejected. If the duplicate search fails,
no CRM write occurs and the pending event is retried.
However, this lookup is not an atomic uniqueness constraint: simultaneous
replicas, a network timeout during creation, or a crash between the lookup and
write can still create duplicates. Exactly-once delivery is **not guaranteed**
until a CRM-side atomic uniqueness guarantee or reconciliation workflow exists.
The portal also does not expose `TYPE_ID` in `crm.lead.fields`; that field is
omitted. Lead and contact `COMMENTS` contain the original Telegram text only;
parsed metadata is not appended to the comment.

PII in routine logging has been reduced. Existing Railway historical logs are
not modified or deleted by this patch.
