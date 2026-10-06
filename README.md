# SessionKit

SessionKit is a Go library for moving a stopped Codex conversation between
workspaces. It captures one native rollout, installs identical bytes into an
isolated Codex home, and reports how to resume its original UUID. It does not
launch Codex, execute tools, move repository files, or copy a whole agent home.

The first profile is `codex/paginated@0.160.0`, pinned to
[Codex 0.160.0](https://github.com/openai/codex/tree/a956835d020762cb2b570053af06f643a11c0ecc).
Layout is detected from the records and path, then the writer version is checked.
Changing a version label does not make a legacy file supported.

| Term | Meaning |
| --- | --- |
| Cold handoff | Stop the source writer, capture saved conversation data, then launch a fresh destination client. |
| Rollout | Codex's JSONL conversation file, including native context and tool records. |
| Bundle | A rollout plus a manifest with its identity, inspection, size, and SHA-256. |
| Compaction | Native replacement history used to shorten model context. The original bytes remain intact. |
| Harness | The coding-agent program. It is distinct from its model provider. |

| Contract | Supported behavior |
| --- | --- |
| Source | `sessions/YYYY/MM/DD/rollout-<timestamp>-<UUIDv7>.jsonl`, with a single first `session_meta`, `history_mode: paginated`, and `cli_version: 0.160.0`. |
| Records | Every record has a string timestamp and an unsigned integer ordinal, in non-decreasing order. Ordinal gaps are reported. |
| Identity | Keep the source UUID. Refuse any matching filename under destination `sessions/` or `archived_sessions/`. |
| Context | Preserve all bytes, unknown record types, function/custom tool calls/results, and complete compaction records. Reject incomplete compactions and orphan tool calls or outputs in effective history, including replacement history. Local-shell and tool-search records are rejected until native validation is available. |
| Lineage | Reject non-null `history_base`, fork/parent/subagent fields, internal and subagent sources, subagent activity/collaboration items, inter-agent communication records, additional metadata records, and `_<rolloutId>` filename suffixes. |
| Relocation | No rewrites. The caller overrides cwd, runtime workspace roots, provider, and policy at resume. |
| Quiescence | Acquire Codex's coordination lock, then try the thread's writer lock. An active writer, unfinished turn, or `exec_command` without a terminal `CommandExecution` completion is a typed rejection. Hold the thread lock through copying and digest rechecks using the same home descriptor. |
| Installation | Private staging, file fsync, hard-link publication without replacement, directory fsync, and digest verification. Probe the destination thread's writer lock while holding Codex's coordination lock across publication. |
| Omitted metadata | Thread name, git metadata, and memory mode are reported as omitted. Their paginated metadata lives outside the selected rollout. Any historical metadata already in the file remains byte-identical. |
| Unsupported files | Legacy, compressed `.jsonl.zst`, archived sources, and multi-file lineage. |
| Host | Linux and macOS, with Go 1.26 or later and a filesystem supporting `flock`, hard links, and directory fsync. |

Use the pinned client for sessions intended to migrate. A state database must be
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
    Root: destinationHome,
    WorkingDir: destinationWorkspace,
    JournalDir: journalDir, // existing directory outside destinationHome
}
plan, err := sessionkit.PlanInstall(ctx, bundle, dst)
if err != nil {
    return err
}
// Review plan.ResumeHints and plan.Omitted. Persist json.Marshal(plan) privately
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
directory modes alone. Inspection changes only lock coordination files, never
the rollout. Planning performs no writes.

`OpenBundle` checks the manifest schema, re-inspects the rollout, and verifies
that every summary and component digest matches. A bundle contains:

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
Use the pinned client to produce native payloads. Hand-edited nested payloads
require separate native schema validation.

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
inode. If execution stopped after removing that evidence but before recording
`published`, it returns a clear `Unknown` instead of guessing. Once `published`
or `verified` is recorded, retries can verify the target without the bundle.
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

`Inspect`, `Capture`, and `OpenBundle` accept caller budgets. `PlanInstall`,
`Install`, and `Verify` use the fixed defaults above. Raising a capture budget
does not raise installation limits; large bundles captured with custom limits
can exceed installation's cumulative read budget. Configurable installation
budgets are a separate API follow-up.

Manifests and journals also have fixed 1 MiB and 16 KiB caps. A capture reads the
rollout four times for source hashing, copying, source rechecking, and inspection;
budget total reads accordingly. Duplicate keys, invalid Unicode, malformed JSON,
and a missing final newline are rejected. JSON numbers never pass through
`float64` in the library.

There is no automatic redaction. Conversations and tool outputs can contain
secrets even though authentication files are excluded. Inspection warns about
repository URL userinfo, encrypted content, and workspace roots outside the
recorded cwd. Encrypted reasoning or compaction data may be provider-bound.
Byte preservation does not prove that another provider will accept it.

Run the ordinary checks without a native client:

```sh
make test vet lint
go test -race ./...
```

The separate native gate requires `curl`, `tar`, a SHA-256 utility, and `sqlite3`
for a read-only test assertion. Its installer downloads the exact native npm
payload into a scratch prefix and verifies both archive and binary SHA-256:

```sh
export SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh)"
make native-gate
```

The gate fails if the variable is absent or the binary digest differs. It passed
on Linux amd64 and macOS arm64. Linux arm64 and macOS amd64 digests are recorded,
but those host gates have not been run. CI runs ordinary checks and native gates
on Linux and macOS.

The native tests use fresh homes, a fake API key, and a local scripted Responses
provider. They verify original roles, ordering, multiplicity, tool arguments and
nonce output, explicit effective destination settings, native SQLite backfill,
new appended replies and ordinals, restart, compaction replacement history,
return into a third home, source preservation, collisions, and active writers.
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
Legacy compatibility, shared-home identities, subagents, and cross-harness
conversion need separate profiles and native evidence before support expands.

Licensed under [MIT](LICENSE).
