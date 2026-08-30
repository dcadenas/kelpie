# skills/

Three skills live here. They are read by agents, not by Kelpie, and they are not
all shipped the same way.

| Directory | Audience | Ships in the crate |
| --- | --- | --- |
| `kelpie/` | Anyone *using* Kelpie to coordinate agents | Yes |
| `kelpie-deploy/` | Anyone working *on* Kelpie — put a build in front of a running fleet | No |
| `kelpie-diagnose/` | Anyone working *on* Kelpie — read the durable record when an operation looks wrong | No |

Only `skills/kelpie/SKILL.md` is listed in `Cargo.toml`'s `include`, so adding a
maintainer skill never changes what `cargo publish` uploads.

## `kelpie/SKILL.md` is also compiled in

`src/cli.rs` embeds it with `include_str!` and `kelpie --skill` prints it.
`tests/skill_package.rs` asserts the printed text matches this file byte for
byte, so edit the file — never a copy of it — and run:

```sh
cargo test --test skill_package
```

The frontmatter `description:` is the only part an agent reads before deciding
whether to load the skill, so it has to name the *moments* that need the skill,
not just the commands the skill documents. An agent that already knows
`kelpie ask` will not load a skill described as "how to send a message"; it will
load one that says "read this before waiting on an answer you are owed".

## Installing them

Install into whatever agent you use:

```sh
npx skills add . -a '*' -s kelpie-deploy -s kelpie-diagnose
```

The per-agent trees that creates are generated and git-ignored.

**Install as a symlink back to this directory, not as a copy.** On this machine
the layout is one shared tree that each agent points into:

```
~/.agents/skills/kelpie  ->  ~/code/kelpie/skills/kelpie
~/.claude/skills/kelpie  ->  ../../.agents/skills/kelpie
~/.config/opencode/skills/kelpie  ->  ../../../.agents/skills/kelpie
```

so an edit here is live for every agent immediately, with no reinstall step.

A copy silently rots instead. On 2026-08-17 a review worker slept and polled
`kelpie pending` for an answer it had been told would be pushed to it — the exact
mistake `kelpie/SKILL.md` § "Ask vs tell" forbids. The skill was installed and
visible in that session, and the rule had been in this file for days; the
installed *copy* was two days stale and the agent had never loaded it anyway.
Two failures, one of them purely mechanical, and only the mechanical one is
cheap to prevent for good.
