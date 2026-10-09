# Native Codex gate

The gate runs the actual Codex 0.160.0 client against a local scripted Responses
provider. Each client receives a private `HOME` and `CODEX_HOME`, a fake API key,
and an explicit provider configuration. No production provider is contacted.

```sh
export SESSIONKIT_CODEX_BIN="$(./scripts/install-codex.sh)"
make native-gate
```

Go, `curl`, `tar`, a SHA-256 utility, and the `sqlite3` CLI must be installed.
The installer fetches the native payload from the exact platform package of
`@openai/codex@0.160.0` into a new scratch prefix. It verifies both the npm archive
and the extracted binary before printing the binary path. An optional first
argument selects a new, nonexistent prefix. Keep or delete that prefix after use.
The gate independently hashes the binary. Both an absent binary and a mismatched
digest are failures, including when `go test -tags=native` is invoked directly.

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
by the read-only CLI in this test, never by the library.

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

The client source is pinned to
[`a956835d020762cb2b570053af06f643a11c0ecc`](https://github.com/openai/codex/tree/a956835d020762cb2b570053af06f643a11c0ecc).
The SSE events follow `codex-rs/core/tests/common/responses.rs`; app-server
parameters follow `codex-rs/app-server-protocol/src/protocol/v2/thread.rs`.

The npm artifacts were downloaded and hashed on 2026-10-02. These are binary
digests; the corresponding archive digests are also pinned in the installer.

| Platform | Binary SHA-256 |
| --- | --- |
| Linux x64 | `12eb3e81114588aca3b7998f4f19e8997b056aca08e57a7ca7c8a3ec8c652aad` |
| Linux arm64 | `50b06603bdcdac39b714f5c3e68583c002b8ad8779ebfdaaf4932ff016b379c0` |
| macOS x64 | `5383ef71dd1bd8d2f3658c04a219e2cf165c7969aebd0cceced1bc9f0f68877f` |
| macOS arm64 | `112fae7a5a1223e673c8a1791d32338f37df8b527ff1159bb8adac6c4dbf1b4b` |
