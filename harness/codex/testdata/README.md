# Fixture provenance

`basic/` and `compacted/` contain rollouts written by the real macOS arm64 Codex
0.160.0 binary, source commit `a956835d020762cb2b570053af06f643a11c0ecc`. The binary
SHA-256 is `112fae7a5a1223e673c8a1791d32338f37df8b527ff1159bb8adac6c4dbf1b4b`,
verified against the native npm package. Generation ran on 2026-10-02 local time.
The rollout's UTC timestamps cross into 2026-10-03; its original local-date
directory and canonical filename are preserved.

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

Only fixture preparation changes these generated records. The temporary workspace
path becomes `/sessionkit/source-workspace`, the temporary home becomes
`/sessionkit/source-home`, and `session_meta.git` is removed if present. The
generator serializes JSON objects after those substitutions. UUIDs, timestamps,
ordinals, model records, and native output are otherwise retained. SessionKit
capture and install never perform this sanitization or serialization.

| Fixture | Bytes | SHA-256 |
| --- | ---: | --- |
| basic | 16066 | `3a56421c8af31a3c003bed3f8d10fce30f8df95b3f9b63288b9602fe74b00c4d` |
| compacted | 25405 | `f567be1bfe2bf86b50d5a360c266a86f0a6f37fb848ae8cb15bc5433d4750925` |

To regenerate with the pinned binary:

```sh
export SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh)"
SESSIONKIT_UPDATE_FIXTURES=1 go test -tags=native -run '^TestGenerateFixtures$' -count=1 -v ./native/codex
```

Regeneration creates new UUIDs and timestamps, replaces only the generated
rollouts in these two fixture directories, and requires corresponding golden
updates. Run ordinary tests and `make native-gate` after reviewing the changes.
Rejection cases are derived in Go tests from a valid profile so each test isolates
the field or path under examination.
