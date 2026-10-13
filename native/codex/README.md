# Native Codex gate

The gate runs an actual pinned Codex client, 0.160.0 or 0.162.1, against a local
scripted Responses provider. Each client receives a private `HOME` and
`CODEX_HOME`, a fake API key, and an explicit provider configuration. No
production provider is contacted. Run it once per release:

```sh
export SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh 0.162.1)"
make native-gate
```

Go, `curl`, `tar`, a SHA-256 utility, and the `sqlite3` CLI must be installed.
The installer's required first argument names the release. It fetches the native
payload from the exact platform package of `@openai/codex@<release>` into a new
scratch prefix and verifies both the npm archive and the extracted binary before
printing the binary path. An optional second argument selects a new, nonexistent
prefix. Keep or delete that prefix after use. The gate independently hashes the
binary and selects the release whose pinned digest for this platform it matches.
That release chooses the fixtures under `harness/codex/testdata/<release>` and
the destination `CLIVersion`. An absent binary, a digest that matches no pinned
release, and a `codex --version` mismatch are failures, including when
`go test -tags=native` is invoked directly.

The scripted tool call only prints a fixed nonce. The gate uses
`--dangerously-bypass-approvals-and-sandbox` for `exec`, and explicit `never` /
`danger-full-access` app-server settings. These settings prove rollout format
compatibility; they do not prove sandbox enforcement or authorization policy.

The handoff test captures source A, installs B, verifies the effective app-server
resume response, continues B, restarts with `codex exec resume <uuid>`, captures
B, and installs and resumes C beside workspace A. It checks the original role
order and multiplicity, tool name, call ID, arguments and output. It also checks
unchanged rollout prefixes, increasing ordinals, appended workspace and permission
settings, and native creation of the paginated SQLite row. SQLite is opened only
by the read-only CLI in this test, never by the library. Codex 0.160.0 records the
appended workspace roots in `turn_context`. Codex 0.162.1 omits that field, so its
gate requires the field's absence and finds the roots in the appended
`world_state` filesystem context.

`exec resume` cannot return the effective configuration or accept an explicit
`runtimeWorkspaceRoots` parameter. The first destination resume therefore uses
app-server with those roots, cwd, provider, approvals reviewer, approval policy,
and sandbox passed explicitly, then checks the response. The later restart and
return exercise the explicit UUID CLI command with destination configuration.

The original A is resolved with native `thread/read`, checking its exact path,
preview, and paginated profile while its digest remains unchanged. Sending a new
prompt through `exec resume` in A would append to it. Paginated `thread/read`
does not hydrate model history or materialize item projection on its own.

Separate cases resume a complete native compaction, reject a legacy rollout
before contacting the client, and try to capture while a live native writer is
blocked at the local provider. The compaction request must contain the replacement
summary and must omit the original tool pair and assistant reply.

`TestNativeInterruptedHandoff` blocks a real turn at that provider, sends
`turn/interrupt` with the exact thread and turn IDs, and checks the native
`interrupted` completion and matching `turn_aborted` record with reason
`interrupted`. After native unload and process exit, it captures the stopped
source, opens the bundle, installs into a fresh home, and resumes through
app-server. The actual resumed provider request must contain the original user,
tool call, tool output, assistant reply, interrupted user input, and new input
exactly once and in order. The undelivered scripted reply must be absent. The
test also verifies effective destination settings, appended ordinals and reply,
SQLite backfill, and unchanged source bytes and mtime.

`TestNativeKilledTurnRejected` waits until the blocked turn's `task_started`
record is persisted, then sends SIGKILL without interrupt or unload. The stopped
rollout must have no matching completion or abort. Capture must fail closed with
`in_flight_turn` at that start ordinal, leave no bundle, preserve source bytes and
mtime, and make no further provider request. It does not repair or resume an
unfinished turn. These cases generate records with the pinned client at runtime;
no fixture is edited and no supported format or recovery rule changes. Other
accepted abort reasons remain covered by parser tests, not by this native gate.

`TestNativeProviderStateHandoff` continues the basic fixture with `exec resume`.
The provider returns a reasoning item with a fixed `encrypted_content` value and
an assistant reply, which the client persists. Capture without
`Source.AllowProviderState` must fail with `encrypted_content` at that record's
ordinal and publish no bundle. Capture with the opt-in reports one reasoning item.
Planning into a fresh home must fail with `provider_state` until
`Destination.AllowProviderState` is set. After installation and app-server resume,
the actual provider request must contain the original user, tool call, tool output,
assistant reply, second user prompt, reasoning item, second reply, and new input
exactly once and in order. Its single reasoning item must carry the same
`encrypted_content` string. Compaction items with encrypted content are covered by
parser tests only; the scripted provider does not produce remote compactions.

The subagent cases need no extra configuration. `multi_agent` is a stable feature
enabled by default in both pinned clients, so the fixture config sets no feature
flag for it, and `multi_agent_v2` stays disabled by default. The scripted provider
routes concurrent requests by content: parent requests carry the parent prompt
and are matched by their newest tool output, and the child's request carries only
its own prompt. The model calls the `multi_agent_v1` namespace tools.

A child's completion injects a `<subagent_notification>` user message into its
parent. During a running parent turn it is pending input for the next request,
so the script orders it: a closing parent continues only after the child's turn
is persisted, and an open child answers only after the parent's turn has ended.

`TestNativeSubagentParentHandoff` starts a parent through app-server in home A.
The parent calls `spawn_agent`; the child answers; the parent calls `wait_agent`
and `close_agent` with the child ID from the native spawn output and replies.
After unload, A holds both rollouts, and the parent's `spawn_agent`, `wait`, and
`close_agent` items are `completed` with that child as receiver. Capture selects
only the parent, and the manifest and plan report `subagent threads (1)`. After
installation into a fresh home B and app-server resume with explicit settings,
the actual provider request must contain the parent prompt, the three agent tool
pairs, the parent reply, and the new prompt exactly once and in order. The turn
completes with a newly persisted reply, SQLite backfills the row, and B holds no
child rollout. A further turn calls `send_input` with the old child ID; the native
tool output must be `agent with id <child> not found`, the turn must complete, and
the reply after it must persist. The resulting `failed` `send_input` item is then
accepted by a second capture from B. Source bytes and mtime stay unchanged.

`TestNativeSubagentOpenRejected` runs the same spawn, but the parent replies
without waiting or closing. The child then answers, and the idle parent records
its notification without a new turn. Capture must fail with exactly one
`open_subagent` rejection at the spawn item's ordinal, publish no bundle,
preserve the source, and make no provider request.

Run just these cases with the same digest gate:

```sh
SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh 0.162.1)" \
  go test -count=1 -tags=native -timeout=3m ./native/codex \
  -run 'TestNative(InterruptedHandoff|KilledTurnRejected|ProviderStateHandoff|SubagentParentHandoff|SubagentOpenRejected)$' -v
```

The client sources are pinned to
[`a956835d020762cb2b570053af06f643a11c0ecc`](https://github.com/openai/codex/tree/a956835d020762cb2b570053af06f643a11c0ecc)
for 0.160.0 and
[`092d3acd6bec3e3a14bdc7e7a2810ab628ab759d`](https://github.com/openai/codex/tree/092d3acd6bec3e3a14bdc7e7a2810ab628ab759d)
for 0.162.1. The SSE events follow `codex-rs/core/tests/common/responses.rs`;
app-server parameters follow `codex-rs/app-server-protocol/src/protocol/v2/thread.rs`
and `turn.rs`. The event shapes and thread parameters used here are unchanged
between the releases; `turn/start` gains only optional turn-attribution fields.

The 0.160.0 npm artifacts were downloaded and hashed on 2026-10-02, and the
0.162.1 artifacts on 2026-10-12. Each 0.162.1 archive also matched the `sha512`
integrity in its npm registry metadata before hashing. These are binary digests;
the corresponding archive digests are also pinned in the installer.

| Platform | 0.160.0 binary SHA-256 | 0.162.1 binary SHA-256 |
| --- | --- | --- |
| Linux x64 | `12eb3e81114588aca3b7998f4f19e8997b056aca08e57a7ca7c8a3ec8c652aad` | `628e35487d888a28f0427fc37501da529dd3d81fcbad7a2c3421c1e8670d92e1` |
| Linux arm64 | `50b06603bdcdac39b714f5c3e68583c002b8ad8779ebfdaaf4932ff016b379c0` | `071cbe8d821cf1b02b6d8ba6c8b7991b228fcb8519dcb68a37d31222bb8cabe5` |
| macOS x64 | `5383ef71dd1bd8d2f3658c04a219e2cf165c7969aebd0cceced1bc9f0f68877f` | `f86b8cfd3de94dc70763d251da3ff292e94f4a53ad41b81faab3619a7bcb626c` |
| macOS arm64 | `112fae7a5a1223e673c8a1791d32338f37df8b527ff1159bb8adac6c4dbf1b4b` | `74c6a6d263f40d65f99c27a815275f5b4ed0bcc2baecc396b4cf3255498bdfc0` |
