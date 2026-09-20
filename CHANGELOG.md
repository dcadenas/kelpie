# Changelog

Notable changes per released version, newest first. Versions are the ones
`kelpie --version` / `kelpied --version` report and `v<version>` tags in git.

Entries say what an operator has to do, not what a commit touched. `just
release` refuses a version with no section here.

## 0.2.0-alpha.11

`kelpie replies-ack` now sends `message_id` as a JSON number. The previous
client always failed with `invalid_request` (string id), so a pull-reply ask
never resolved. Install the new `kelpie` client. No schema change.

## 0.2.0-alpha.10

`inbox.delivery` now includes `scheduled_at_ms` and `created_at_ms`. `message_id`
is create order, not offer order: a delayed tell can arrive with a lower id than
traffic you already ACKed. Do not treat `message_id` as a high-water cursor;
ACKed rows are never re-offered. If a socket waiter dropped delayed tells that
way, stop. No schema change; restart kelpied.

## 0.2.0-alpha.9

`KELPIE_SOCKET` now selects the Kelpie socket for both binaries when set (an
empty value is ignored), with `--socket` still winning. If you set the variable
while it was ignored, unset it or point it at the daemon you mean. Repeating a
`tell` or `reply` idempotency key aimed at a socket waiter now returns the
stored message and its current delivery outcome instead of an internal UNIQUE
error, so a client that retried with a fresh key can retry with the original
one. No schema change; restart kelpied.

## 0.2.0-alpha.8

Message prompts ask Herdr to watch the target after the write. A receipt can
now carry `submission=stalled` or `submission=unobserved`, and a stalled write
raises an operator notice. Nothing is resent and the obligation stays open: a
stalled word means Herdr wrote the message but observed no agent activity, and
the recipient may still see it later. Do not resend on that word. Observed
submissions change nothing in the receipt. No schema change; restart kelpied.

## 0.2.0-alpha.7

Reverse replies can stay off the waiting pane. At `ask` or `start --ask` time,
pass `--reply-delivery pull`, then consume them with `replies-claim`, `replies`,
and `replies-ack`. Schema 31. Existing asks stay inject. Restart kelpied to
migrate.

## 0.2.0-alpha.6

`kelpie who NAME --resolve` selects the logical agent a name-keyed host should
continue and includes the full claimant and unresolved-ask picture. Handoff can
accept a name only while that name uniquely identifies a Ready incarnation.

## 0.2.0-alpha.5

IDs are positive integers, unique within each record type. The JSON protocol
encodes them as numbers. UUID IDs are rejected. Schema 30.

Reminder envelopes are compact. Unanswered asks default to a forty-five-minute
reminder (twenty minutes in alpha.4).

**Action**: one-way. Stop kelpied, run the new `kelpied` with `--migrate-only`
against the live database, then start. After recovery, run `kelpie who` and
`kelpie pending` for current IDs. Open asks survive under new integer IDs. If
you only kept a UUID for an ask, ask its sender to send the ask again.

crates.io `0.2.0-alpha.4` is a different program (UUID IDs, schema 29,
twenty-minute reminders). Installing that version does not get this build.

## 0.2.0-alpha.4

UUID IDs, schema 29, twenty-minute reminder default. Negotiates Herdr endpoint
generation 1 and falls back to wire protocol 20. The shipped skill splits
procedures into `skills/kelpie/references/`.

Published 2026-09-08. A later git commit reused this version string for the
integer-ID work; that reuse is why alpha.5 exists.

## 0.2.0-alpha.3

Unanswered asks default to a twenty-minute reminder (five minutes before).
Receivers can snooze or widen an ask's interval. Schema 29.

## 0.2.0-alpha.2

After bind, recover retries restored occupants that were not ready on the first
pass, so a daemon restart can continue them instead of leaving them lost.

## 0.2.0-alpha.1

First public crates.io prerelease after yanking 0.1.0. UUID IDs. The crate name
is `kelpie-herdr` because `kelpie` is taken.

## 0.1.0

Yanked. Do not install it. A bare `cargo install kelpie-herdr` still resolves
here, so the version requirement is required.
