# Identity and fleet inspection

Read when performing inspection operations beyond the entry procedure.

Contents: [Method details](#method-details).

## Method details

- `who`: report one identity and its recorded attribution. Name it
  with a live name, `--pane`, `--agent-id`, or `--incarnation-id`; the default
  is your own pane. It also resolves an active socket waiter by name, where
  `incarnation_id` and attribution are absent. Add `--history` to a name to see
  every claimant and unresolved obligation. `requested` is what a launch asked
  for and is never proof of
  what served a turn; `observed` is adapter evidence. `observed none` means
  nothing was observed, which is not the same as an observed `undetermined`
  field. Adapters exist for `claude`, `codex`, and `opencode`; other kinds are
  `undetermined`. Do not report your own model as observed attribution.
- `who NAME --resolve`: name the logical agent a name-keyed host should
  continue. A unique Ready agent or active socket waiter wins. If none is
  addressable, the newest claimant wins by creation time then logical-agent ID,
  and the result includes every claimant and unresolved obligation. Live
  ambiguity and an unclaimed name fail closed. Use the returned logical ID with
  `start --logical-id` after the name is free; resolution does not free a Herdr
  name held by a dead pane.
- `who --refresh`: observe again and append the result. A backend may
  record its serving model only after its first turn, so an agent that was
  `undetermined` at startup becomes knowable later. Refreshing never rewrites an
  earlier observation and never guesses; when it still cannot tell,
  `undetermined_because` distinguishes an agent that has produced no turn yet,
  which may become knowable later, from a backend with no adapter, which never
  will. Neither is a reason to sit and wait; observe again next time you have
  business with that agent.
- `report`: every logical agent, incarnation, and reply obligation Kelpie holds,
  as a parentage tree — indent means "started by the line above". Each line
  attributes its facts: `kelpie=` is the state Kelpie recorded for the newest
  incarnation, `herdr=` is Herdr's live status for that exact pane and terminal.
  They can disagree, and the disagreement is the point: `kelpie=lost herdr=idle`
  means a runtime is alive that Kelpie can no longer address. `incarnations=`
  counts how many runtimes this one logical agent has been bound to, because a
  logical agent outlives them. Incarnations come newest first. It reports facts
  and never judges them, so decide for yourself what a state means. `--live`
  adds the Herdr column, taken at report time rather than stored. `--active`
  keeps only agents that still exist — newest incarnation ready, starting, or
  unknown — plus the ancestors that explain who started them, which is usually
  what you want and a fraction of the output. `--json` gives the graph for
  anything that wants to render it.
