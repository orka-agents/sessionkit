# SessionKit

SessionKit is a Go library for moving a stopped Codex conversation between
workspaces. It captures one native rollout, installs identical bytes into an
isolated Codex home, and reports how to resume its original UUID. It does not
launch Codex, execute tools, move repository files, or copy a whole agent home.

Each profile is pinned to one exact Codex release:

| Profile | Constant | Writer and destination client |
| --- | --- | --- |
| `codex/paginated@0.160.0` | `CodexPaginated` | [Codex 0.160.0](https://github.com/openai/codex/tree/a956835d020762cb2b570053af06f643a11c0ecc) |
| `codex/paginated@0.162.1` | `CodexPaginated162` | [Codex 0.162.1](https://github.com/openai/codex/tree/092d3acd6bec3e3a14bdc7e7a2810ab628ab759d) |

Layout is detected from the records and path, then the recorded `cli_version`
selects the profile. There is no fallback to the nearest release. Changing a
version label does not make a legacy file supported.

Both releases declare the same rollout, response-item, event, turn-item,
abort-reason, and session-source enums, serde aliases, writer-lock protocol,
and state database names, so both profiles apply the same validation rules.
Codex 0.162.1 adds optional event fields, a `partial_answer` assistant phase,
and persisted `additional_tools` items. It drops workspace roots from
`turn_context` and stdout/stderr copies from command completions. SessionKit
does not read any of those fields. A bundle installs only into the client
release that wrote it.

| Term | Meaning |
| --- | --- |
| Cold handoff | Stop the source writer, capture saved conversation data, then launch a fresh destination client. |
| Rollout | Codex's JSONL conversation file, including native context and tool records. |
| Bundle | A rollout plus a manifest with its identity, inspection, size, and SHA-256. |
| Compaction | Native replacement history used to shorten model context. The original bytes remain intact. |
| Harness | The coding-agent program. It is distinct from its model provider. |

| Contract | Supported behavior |
| --- | --- |
| Source | `sessions/YYYY/MM/DD/rollout-<timestamp>-<UUIDv7>.jsonl`, with a single first `session_meta`, `history_mode: paginated`, and `cli_version` of exactly `0.160.0` or `0.162.1`. |
| Records | Every record has a string timestamp and an unsigned integer ordinal, in non-decreasing order. Ordinal gaps are reported. |
| Identity | Keep the source UUID. Refuse any matching filename under destination `sessions/` or `archived_sessions/`. |
| Context | Preserve all bytes, opaque passive native records, function/custom tool calls/results, and complete compaction records. Reject types outside the pinned native enums, `Other` response items, request-only compaction triggers, incomplete compactions, and orphan tool calls or outputs in effective history, including replacement history. Provider file IDs and external media URLs are rejected; inline data URLs remain in the rollout. Local-shell and tool-search records are rejected until native validation is available. Non-null `encrypted_content` is rejected unless capture and destination both allow provider state; non-null `encrypted_function_args` is always rejected. |
| Lineage | Reject non-null `history_base`, fork/parent/subagent fields, inherited/sender response metadata, internal and subagent source classifications, subagent activity items, collaboration events, started collaboration items, `agent_message` response items, inter-agent communication records, additional metadata records, and `_<rolloutId>` filename suffixes. Ordinary local retained-source metadata stays byte-identical. |
| Subagent parents | Accept a parent's multi-agent v1 calls (`spawn_agent`, `send_input`, `wait_agent`, `close_agent`, `resume_agent` in the `multi_agent_v1` namespace). Each persisted `CollabAgentToolCall` `item_completed` must belong to the selected thread, have status `completed` or `failed`, name a v1 tool, and match a pending call whose output follows in effective history. `spawn_agent` and `resume_agent` calls in that namespace, or unnamespaced as in v2, require that item. Every child named by a spawn, or by a completed resume, must be closed later by a completed `close_agent` item; otherwise capture fails with `open_subagent` at the opening item's ordinal. A multi-agent call whose output a compaction discarded is rejected. Child threads are never captured. |
| Relocation | No rewrites. The caller overrides cwd, runtime workspace roots, provider, and policy at resume. |
| Quiescence | Acquire Codex's coordination lock, then try the thread's writer lock. An active writer, unfinished turn, or `exec_command` without a terminal `CommandExecution` completion is a typed rejection. Hold the thread lock through copying and digest rechecks using the same home descriptor. |
| Installation | Private staging, file fsync, hard-link publication without replacement, directory fsync, and digest verification. Probe the destination thread's writer lock while holding Codex's coordination lock across publication. |
| Omitted metadata | Thread name, git metadata, and memory mode are reported as omitted. Their paginated metadata lives outside the selected rollout. Any historical metadata already in the file remains byte-identical. A subagent parent also reports `subagent threads (N)`, the number of distinct child threads it opened. |
| Unsupported files | Legacy, compressed `.jsonl.zst`, archived sources, and multi-file lineage. |
| Host | Linux and macOS, with Go 1.26 or later and a filesystem supporting `flock`, hard links, and directory fsync. |

A subagent parent moves alone. Its child rollouts and the source's spawn edges
stay in the source home, and neither pinned client restores children on a
top-level resume. In the destination, a later multi-agent call with an old child
ID is a model-visible tool error such as `agent with id … not found`, not a turn
failure. The parent's `<subagent_notification>` messages and `world_state`
subagent lists remain byte-identical history. Multi-agent v2 tools and child
threads themselves remain unsupported.

Use a pinned client for sessions intended to migrate. A state database must be
available when the source thread is created so Codex writes paginated history.
Ephemeral or database-less sessions can write legacy history and are rejected.
The destination starts in a fresh or isolated `CODEX_HOME`. Codex itself rebuilds
the `state_5.sqlite` row and `thread_history_1.sqlite` projection when loading the
installed file. SessionKit never opens those databases, `session_index.jsonl`,
or `history.jsonl`.

An isolated destination is a requirement, not something filename scans can prove.
A stale paginated database row can shadow the installed file without filename
fallback. Keep `CODEX_SQLITE_HOME` unset or directed at the destination's private
database directory. A return to the source machine needs another isolated home
beside the preserved original.

The public operations are `Inspect`, `Capture`, `OpenBundle`, `PlanInstall`,
`Install`, and `Verify`. For example, inside a caller that supplies `ctx`,
`sourceHome`, `threadID`, and prepared destination directories:

```go
src := sessionkit.Source{
    Harness: sessionkit.Codex,
    Root: sourceHome,
    ThreadID: threadID,
}
bundle, err := sessionkit.Capture(ctx, src, sessionkit.CaptureOptions{
    BundleDir: bundleDir, // must not exist; its parent must exist
})
if err != nil {
    return err
}

dst := sessionkit.Destination{
    Harness: sessionkit.Codex,
    CLIVersion: "0.162.1", // the caller's verified destination binary; must equal the bundle's source version
    Root: destinationHome,
    WorkingDir: destinationWorkspace,
    JournalDir: journalDir, // existing directory outside destinationHome and bundleDir
}
plan, err := sessionkit.PlanInstall(ctx, bundle, dst)
if err != nil {
    return err
}
// Review plan.ResumeHints, plan.Omitted, and plan.ProviderState. Persist json.Marshal(plan) privately
// if the caller needs retries across process restarts.
receipt, err := sessionkit.Install(ctx, plan)
if err != nil {
    return err // inspect receipt.Outcome; preserve an Unknown receipt and plan
}
_, err = sessionkit.Verify(ctx, receipt, dst)
return err
```

All supplied directories must be absolute. Destination home, workspace, journal,
and the bundle's parent must already exist. Every path component is opened
without following symlinks. Resolve a platform alias such as macOS `/tmp` before
passing a caller-selected directory. Use caller-owned private roots; SessionKit
creates files with mode 0600 and directories with mode 0700, and leaves existing
directory modes alone. The destination home and bundle must not overlap.
Inspection changes only lock coordination files, never the rollout.
The caller supplies the verified destination binary version in `CLIVersion`.
Planning accepts only the tested same-release pairs, `0.160.0` into `0.160.0`
and `0.162.1` into `0.162.1`, and `Install` rechecks the pair against the
re-verified bundle. SessionKit does not launch a binary to discover its version.
Verification also holds Codex's writer coordination and rejects an
active writer. Planning performs no writes.

`OpenBundle` checks the manifest schema, rejects undeclared entries, re-inspects
the rollout, and verifies that every summary and component digest matches.
A bundle contains:

```text
manifest.json
components/rollout.jsonl
```

Keep bundle files private and unchanged during handoff operations.
During capture, the caller must exclusively control the destination bundle name
and its parent directory. Concurrent renaming or replacement during directory
creation requires a separate coordination and publication contract.
`Bundle.Manifest` is a verification snapshot. `PlanInstall` revalidates the files
and `Install` checks the resulting plan's frozen digest before publication.
Concurrent edits to bundle files require caller-owned coordination.
Use a pinned client to produce native payloads. Rollout, response-item, and
event type tags are checked against the pinned profile. Hand-edited nested
payloads require separate native schema validation.

`PlanInstall` freezes the verified manifest digest and original relative path.
It does not recompute date directories from timestamps, because Codex's directory
and filename time zones can differ. JSON serialization retains the plan's bundle
and destination binding. Its digest detects accidental edits; it is not an
authorization token or a signature. The consumer must authorize the destination
and the data it accepts.

The caller launches a fresh client with `ResumeHints.CodexHome`, explicitly
resumes `ThreadID`, and supplies its own configuration. For app-server, start
with `ResumeHints.AppServerParams`, which contains `threadId`, `cwd`, and
`runtimeWorkspaceRoots`. Add destination-owned `modelProvider`, `approvalPolicy`,
`approvalsReviewer`, and `sandbox` settings and verify the effective values in
the `thread/resume` response before starting a turn. Provider definitions,
credentials, tools, MCP connections, and network policy also belong to the
destination. Saved cwd and policy occur in several native record types; replacing
one metadata field would not relocate the session safely.

`ResumeHints.NativeCommand` contains the base `codex exec resume <UUID>` command.
It is a hint, not a complete launch configuration. Supply explicit destination
settings and cwd through the pinned client's supported options. Picker visibility
and `resume --last` are not promised because they filter by provider ID. Vekil's
explicit UUID return command has this shape:

```sh
CODEX_HOME=<isolated-home> vekil launch codex -- resume <thread-id>
```

The launcher owns fresh provider settings. Its per-launch provider IDs need
[separate selection validation](https://github.com/sozercan/vekil/issues/418).
Orka's [Codex/ACP pin update](https://github.com/orka-agents/orka/pull/720) is
separate from the [capture/restore and CLI integration](https://github.com/orka-agents/orka/issues/721).
This library does not make `orka session migrate` available.

Each install has `JournalDir/<OperationID>.json` and an operation lock file.
The journal advances through `planned`, `staged`, `published`, and `verified`
using atomic replacements. It is not a session catalog. `RejectedBeforeMutation`
means this attempt did not publish a rollout; private staging directories and
receipts may exist. `Installed` means the target bytes were verified. `Unknown`
means publication may have happened. Preserve the plan, bundle, journal, and
staging evidence and retry the same plan. Concurrent uses of that plan serialize.

A staged retry can prove publication when target and staged file are the same
inode. `published` is recorded before that staged witness is removed, so a
retry after any later interruption removes the leftover witness and verifies
the target without the bundle.
`Verify` compares a completed receipt to installed bytes without starting Codex.
After a native turn appends new data, that earlier digest is expected to differ;
capture a new bundle for the next handoff.

Errors support `errors.As` with `RejectionError`, `ActiveWriterError`,
`CollisionError`, `BudgetError`, `IntegrityError`, and `UnknownOutcomeError`.
Rejections identify components and ordinals without quoting conversation bodies.
Metadata reports and manifests still contain private paths and provider IDs.
Keep them private with the bundle.

One budget covers each operation's reads, parsed records/nodes, temporary output,
and elapsed time. Zero fields choose these defaults; negative limits reject:

| Limit | Default |
| --- | ---: |
| Total bytes read, including repeated verification reads | 1 GiB |
| One JSONL line, excluding LF | 64 MiB |
| Parsed records | 100,000 |
| JSON nesting depth | 128 |
| JSON tokens and traversed filesystem entries | 10,000,000 |
| Temporary bytes written/reserved | 1 GiB |
| Elapsed time | 5 minutes |

A canceled attempt that has not begun publication may reset its journal using
an independent one-second cleanup budget with a 16 KiB temporary-write limit.

`Inspect`, `Capture`, and `OpenBundle` accept caller budgets. `PlanInstall`,
`Install`, and `Verify` use the fixed defaults above. Raising a capture budget
does not raise installation limits; large bundles captured with custom limits
can exceed installation's cumulative read budget. Configurable installation
budgets are a separate API follow-up.

Manifests and journals also have fixed 1 MiB and 16 KiB file-size caps. These
limits bound files and I/O, not heap memory. Bounded manifest serialization is a
separate follow-up. A capture reads the rollout three times: hashing while
copying, source rechecking, and inspection. Budget total reads accordingly.
Duplicate keys, invalid Unicode, malformed JSON,
and a missing final newline are rejected. JSON numbers never pass through
`float64` in the library.

There is no automatic redaction. Conversations and tool outputs can contain
secrets even though authentication files are excluded. Inspection warns about
repository URL userinfo and workspace roots outside the recorded cwd.

Non-null `encrypted_content` and `encrypted_function_args` are opaque
provider-bound state and are rejected by default. With `Source.AllowProviderState`,
`Inspect` and `Capture` keep string `encrypted_content` on native `reasoning`,
`compaction`, `compaction_summary`, and `context_compaction` items, including
replacement history entries. Every other position is still rejected, and
`encrypted_function_args` is never accepted. `Inspection.ProviderState` counts the
kept reasoning and compaction items. The manifest records the counts and
`allowProviderState`; `OpenBundle` re-inspects under that recorded policy.
`PlanInstall` rejects a bundle with kept content as `provider_state` unless
`Destination.AllowProviderState` is also set, and `Plan.ProviderState` repeats the
counts for review.

SessionKit neither interprets nor rewrites these bytes. Neither pinned client's
request construction ([0.160.0](https://github.com/openai/codex/blob/a956835d020762cb2b570053af06f643a11c0ecc/codex-rs/core/src/client.rs#L940-L959),
[0.162.1](https://github.com/openai/codex/blob/092d3acd6bec3e3a14bdc7e7a2810ab628ab759d/codex-rs/core/src/client.rs#L942-L961))
strips kept reasoning when the provider or model changes, and the native gate
verifies that a kept reasoning item reaches the resumed request unchanged.
Byte preservation does not prove that the destination provider accepts it;
providers reject state they cannot decrypt. Allow it only when the destination
path drops or accepts provider-bound content, for example through a proxy that
removes state it does not own.

Run the ordinary checks without a native client:

```sh
make test vet lint
go test -race ./...
```

The separate native gate requires `curl`, `tar`, a SHA-256 utility, and `sqlite3`
for a read-only test assertion. Its installer downloads the exact native npm
payload of one pinned release into a scratch prefix and verifies both archive
and binary SHA-256. Run the gate once per release:

```sh
for version in 0.160.0 0.162.1; do
  SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh "$version")" make native-gate
done
```

The gate identifies the release by the binary digest and selects that release's
fixtures and destination version. It fails if the variable is absent, the digest
matches no pinned release for the host, or `codex --version` disagrees.

| Release | Native gate passed on | Digests recorded, gate not run |
| --- | --- | --- |
| 0.160.0 | Linux amd64, macOS arm64 | Linux arm64, macOS amd64 |
| 0.162.1 | Linux amd64, macOS arm64 | Linux arm64, macOS amd64 |

CI runs ordinary checks, and native gates for both releases on Linux and macOS.

The native tests use fresh homes, a fake API key, and a local scripted Responses
provider. They verify original roles, ordering, multiplicity, tool arguments and
nonce output, explicit effective destination settings, native SQLite backfill,
new appended replies and ordinals, restart, compaction replacement history,
return into a third home, source preservation, collisions, and active writers.
A real `turn/interrupt` produces a matching `turn_aborted` record; after native
unload and process exit, capture/install/resume must retain the interrupted user
input and original tool history in the actual provider request. A client killed
with SIGKILL after persisting a blocked turn must instead fail capture with
`in_flight_turn`, preserve the source, and publish no bundle. A scripted reasoning
item with fixed `encrypted_content` must fail capture without the opt-in; with both
opt-ins, the resumed provider request must contain those exact bytes in their
original position. These tests do not repair unfinished turns or expand accepted
abort reasons. A native parent that spawns a child, waits, and closes it is
captured alone, installed, and resumed; its spawn, wait, and close pairs must
reach the provider exactly once and in order, a new reply must persist, and a
`send_input` to the old child ID must return the native `not found` tool output.
A parent that leaves its child open must fail capture with `open_subagent` and
publish no bundle. See the [native gate documentation](native/codex/README.md)
for the focused command.
Source UUID resolution uses read-only `thread/read` so validation cannot append
to the original. App-server verifies explicit resume settings; native exec
exercises restart and return. Sandbox/approval bypass settings are confined to
these isolated fixtures and prove format compatibility only. Consumer permission
and live-provider behavior require their own validation.

Fixture origins and regeneration are recorded in
[harness/codex/testdata/README.md](harness/codex/testdata/README.md).
After intentionally regenerating them, update inspection and normalized plan
goldens with `UPDATE_GOLDEN=1 go test . -run TestGoldenInspectionAndPlan` and rerun
both test layers.

Follow-up scope is tracked in [lineage bundles](https://github.com/orka-agents/sessionkit/issues/2),
[identity and metadata extensions](https://github.com/orka-agents/sessionkit/issues/3),
and [compressed sources and conversion](https://github.com/orka-agents/sessionkit/issues/4).
Legacy compatibility, shared-home identities, subagent child threads, and
cross-harness conversion need separate profiles and native evidence before
support expands.

Licensed under [MIT](LICENSE).
