# Identity and fleet inspection

Read when performing inspection operations beyond the entry procedure.

Contents: [Method details](#method-details).

## Method details

- `who`: report one identity and its recorded attribution. Name it
  with a live name, `--pane`, `--agent-id`, or `--incarnation-id`; the default
  is your own pane. It also resolves an active socket waiter by name, where
  `incarnation_id` and attribution are absent. Add `--history` to a name to see
  every claimant and unresolved obligation, including identities kelpied
  archived after a day wholly dead; `--resolve` still continues those. `requested` is what a launch asked
  for and is never proof of what served a turn. Kelpie reads no harness data, so
  there is no observed model to check; a launch's model setting is best effort.
- `who NAME --resolve`: name the logical agent a name-keyed host should
  continue. A unique Ready agent or active socket waiter wins. If none is
  addressable, the newest claimant wins by creation time then logical-agent ID,
  and the result includes every claimant and unresolved obligation. Live
  ambiguity and an unclaimed name fail closed. After the name is free, continue
  a `herdr_prompt` result with `start --logical-id`. An ended `socket_inbox`
  waiter cannot be reactivated; reconcile its obligations and register a new
  waiter instead. Resolution does not free a Herdr name held by a dead pane.
- `who --refresh`: learn a native session that did not exist when the agent
  bound, from a read-only Herdr snapshot. Some backends allocate their session
  only after the first prompt; recovery continues an identity by it.
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
  keeps only agents that still exist — any incarnation ready, starting, or
  unknown, whatever failed after it — plus the ancestors that explain who started them, which is usually
  what you want and a fraction of the output. `--json` gives the graph for
  anything that wants to render it.
