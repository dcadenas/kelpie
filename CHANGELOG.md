# Changelog

Notable changes per released version, newest first. Versions are the ones
`kelpie --version` / `kelpied --version` report and `v<version>` tags in git.

Entries say what an operator has to do, not what a commit touched. `just
release` refuses a version with no section here.

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
