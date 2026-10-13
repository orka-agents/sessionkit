# Fixture provenance

Each `<release>/` directory holds `basic/`, `compacted/`, and `subagent/`
rollouts written by the real macOS arm64 Codex binary of that release, with
inspection and normalized plan goldens beside them. Each binary SHA-256 was
verified against its native npm package.

| Release | Source commit | Binary SHA-256 | Generated |
| --- | --- | --- | --- |
| 0.160.0 | `a956835d020762cb2b570053af06f643a11c0ecc` | `112fae7a5a1223e673c8a1791d32338f37df8b527ff1159bb8adac6c4dbf1b4b` | 2026-10-02 local time |
| 0.162.1 | `092d3acd6bec3e3a14bdc7e7a2810ab628ab759d` | `74c6a6d263f40d65f99c27a815275f5b4ed0bcc2baecc396b4cf3255498bdfc0` | 2026-10-12 local time |

The 0.160.0 `basic` and `compacted` rollouts' UTC timestamps cross into
2026-10-03, and every 2026-10-12 rollout's into 2026-10-13. Their original
local-date directories and canonical filenames are preserved. Both `subagent`
fixtures were generated on 2026-10-12.

The generator runs app-server in a fresh `HOME` and `CODEX_HOME` with state SQLite
enabled and a local scripted Responses server. It starts a thread, submits one
user prompt, runs the model's advertised `exec_command` shell tool to print
`SESSIONKIT_NONCE_7a8381a91bcf`, and records an assistant reply. The native tool
output is checked for exit code 0 and the nonce before saving. This model does
not advertise the older tool named `shell`.

The generator then resumes that thread and calls `thread/compact/start`. The
stub returns the summary text; the native client writes `compacted` with both
`replacement_history` and `window_number`. The replacement contains the original
user message and the summary, while the earlier tool pair and assistant reply
remain only in the preserved file prefix. There is no encrypted content.
Codex 0.162.1 also writes `world_state` and `turn_context` records after the
compaction, so its compacted fixture is larger.

The `subagent` generator starts a fresh parent thread whose scripted model calls
the default-enabled `multi_agent_v1` tools: `spawn_agent` with a child prompt,
`wait_agent` for the child's scripted reply, and `close_agent`, then replies. The
native client persists the three tool pairs, their `completed`
`CollabAgentToolCall` items, a `<subagent_notification>` user message, and
`world_state` subagent updates. Only the parent rollout is saved; the child's
rollout stays in the temporary home.

Only fixture preparation changes these generated records. The temporary workspace
path becomes `/sessionkit/source-workspace`, the temporary home becomes
`/sessionkit/source-home`, and `session_meta.git` is removed if present. The
generator serializes JSON objects after those substitutions. UUIDs, timestamps,
ordinals, model records, and native output are otherwise retained. SessionKit
capture and install never perform this sanitization or serialization.

| Fixture | Bytes | SHA-256 |
| --- | ---: | --- |
| 0.160.0/basic | 16066 | `3a56421c8af31a3c003bed3f8d10fce30f8df95b3f9b63288b9602fe74b00c4d` |
| 0.160.0/compacted | 25405 | `f567be1bfe2bf86b50d5a360c266a86f0a6f37fb848ae8cb15bc5433d4750925` |
| 0.160.0/subagent | 44374 | `3d458d2a3fad526a5f98c34ae1702ab585280665dfaed63a524c2b50c0358a71` |
| 0.162.1/basic | 16156 | `94c82b4750efb5c4aaed79e679ce8d4f9f3dfc97a85b9fabf64a7d8e028cb77e` |
| 0.162.1/compacted | 33158 | `8e1f8f5b7f0ebfc988cb32745fcbce2854cf734272c219e336d94adfc6b4c695` |
| 0.162.1/subagent | 44570 | `098cb1c8bfa53e8b5fdba927aa7eeb78061fe5baee8feb45ee17999fd44ec734` |

To regenerate one release's fixtures with its pinned binary:

```sh
export SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh 0.162.1)"
SESSIONKIT_UPDATE_FIXTURES=1 go test -tags=native -run '^TestGenerateFixtures$' -count=1 -v ./native/codex
```

Append `/^handoff$` or `/^subagent$` to the `-run` pattern to regenerate only the
`basic` and `compacted` pair or only `subagent`. Regeneration creates new UUIDs and
timestamps, replaces only the generated rollouts in the selected fixture
directories, and requires corresponding golden updates. Run ordinary tests and `make native-gate` after reviewing the
changes. Rejection cases are derived in Go tests from a valid profile so each
test isolates the field or path under examination.
