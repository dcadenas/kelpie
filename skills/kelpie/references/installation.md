# Install or project the skill

Read before installing Kelpie skill files or preparing an isolated agent environment.


The canonical skill is shipped at `skills/kelpie/SKILL.md` and is printed by
`kelpie --skill`. That dump is not how agents discover Kelpie; something must
already know to load this skill or run that command.

Install the skill globally with the open skills CLI:

```sh
npx skills add dcadenas/kelpie --skill kelpie -g
```

The `-g` flag installs globally for the supported agents selected by the user.
Omit it for a project-local installation. If Kelpie is already installed,
rerun the add command or use the skills CLI's update command.

`kelpie --skill` prints the release-matched entry embedded in the installed
binary. For routine messaging that entry is the manual fallback when the skills
CLI is unavailable. Before an operation routed to a reference, obtain the complete
`skills/kelpie/` directory from the source release matching `kelpie --version`.
Keep `SKILL.md` and `references/` together when copying or projecting the skill.
Fresh Herdr sessions do not learn Kelpie from Herdr itself; they
learn it when their agent runtime indexes an installed skill or when the
launching environment explicitly includes Kelpie instructions.

Isolated agent environments must explicitly project the Kelpie skill into their
own skill bundle or prompt. They must not rely only on global skill discovery.
