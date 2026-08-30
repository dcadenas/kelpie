# Security policy

## Reporting a vulnerability

Use GitHub's private vulnerability reporting (Security tab → Report a
vulnerability) on this repository. Please do not open a public issue for
anything you believe is exploitable.

## Scope and honest boundaries

Kelpie is a local, single-user coordination layer. It talks to Herdr over a
Unix socket in your runtime directory and stores its state in a SQLite
database under your state directory.

Know what it does not claim:

- **Kelpie does not authenticate agents.** Sender identity is a same-user
  attribution claim, never a security boundary. Any process running as your
  user can connect to the socket and act as any agent. SPEC.md says this
  explicitly, and anything capability-shaped must be validated by the
  transports above Kelpie, not by Kelpie.
- **Message bodies are delivered as typed.** Envelope metadata cannot be
  forged by message text, but the text itself is untrusted input from
  whoever sent it.

## Supported versions

Only the latest `main` receives security fixes. There are no release branches
to backport to.
