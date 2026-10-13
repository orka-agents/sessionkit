#!/bin/sh
# Install the exact native npm payload of one pinned Codex release for the
# compatibility gate.
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then echo "usage: $0 <0.160.0|0.162.1> [new-scratch-prefix]" >&2; exit 1; fi
version=$1
platform=$(uname -s)
arch=$(uname -m)
case "$platform/$arch" in
  Linux/x86_64) target=linux-x64; triple=x86_64-unknown-linux-musl ;;
  Linux/aarch64|Linux/arm64) target=linux-arm64; triple=aarch64-unknown-linux-musl ;;
  Darwin/x86_64) target=darwin-x64; triple=x86_64-apple-darwin ;;
  Darwin/arm64) target=darwin-arm64; triple=aarch64-apple-darwin ;;
  *) echo "unsupported platform: $platform/$arch" >&2; exit 1 ;;
esac
case "$version/$target" in
  0.160.0/linux-x64) tar_digest=37a41d61c3399182b8c727b77090cc7a1566bd849d0f09070a0bbc6fec4c58dc; binary_digest=12eb3e81114588aca3b7998f4f19e8997b056aca08e57a7ca7c8a3ec8c652aad ;;
  0.160.0/linux-arm64) tar_digest=9286a7e01d500ab224c9e5b4b223b7adf5dd901fba23efd6a438316426798b17; binary_digest=50b06603bdcdac39b714f5c3e68583c002b8ad8779ebfdaaf4932ff016b379c0 ;;
  0.160.0/darwin-x64) tar_digest=d90ef1be135b605c88af1b7595bac768a02c7ea410688240961899655ccb5d2e; binary_digest=5383ef71dd1bd8d2f3658c04a219e2cf165c7969aebd0cceced1bc9f0f68877f ;;
  0.160.0/darwin-arm64) tar_digest=fc789bcd655d903f92e1a23c8dc5315ba38f43b3586eafb7bd3b195970b57466; binary_digest=112fae7a5a1223e673c8a1791d32338f37df8b527ff1159bb8adac6c4dbf1b4b ;;
  0.162.1/linux-x64) tar_digest=7417711678b464eee70664126e39e88bb3c84c2928da0e71f14b56e04ff5c09f; binary_digest=628e35487d888a28f0427fc37501da529dd3d81fcbad7a2c3421c1e8670d92e1 ;;
  0.162.1/linux-arm64) tar_digest=a355051d0c296ed50e42e55aceabf74f017f2f96e223fc7ee2199ea8a60cad4b; binary_digest=071cbe8d821cf1b02b6d8ba6c8b7991b228fcb8519dcb68a37d31222bb8cabe5 ;;
  0.162.1/darwin-x64) tar_digest=e4f53d69d399f1086eb35bc3a47786cbd03dd1f92c85e8217258e36f1470f5ec; binary_digest=f86b8cfd3de94dc70763d251da3ff292e94f4a53ad41b81faab3619a7bcb626c ;;
  0.162.1/darwin-arm64) tar_digest=fe0370027a05237adc0152b482ad8502321f6f9aff1baca3bfa56588923ba9ad; binary_digest=74c6a6d263f40d65f99c27a815275f5b4ed0bcc2baecc396b4cf3255498bdfc0 ;;
  *) echo "unsupported Codex version: $version" >&2; exit 1 ;;
esac

digest() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d ' ' -f 1
  else shasum -a 256 "$1" | cut -d ' ' -f 1
  fi
}

umask 077
if [ "$#" -eq 2 ]; then
  mkdir "$2"
  prefix=$(cd "$2" && pwd -P)
else
  prefix=$(mktemp -d "${TMPDIR:-/tmp}/sessionkit-codex.XXXXXX")
fi
archive="$prefix/codex.tgz"
curl --fail --location --silent --show-error --retry 3 \
  "https://registry.npmjs.org/@openai/codex/-/codex-$version-$target.tgz" -o "$archive"
if [ "$(digest "$archive")" != "$tar_digest" ]; then echo "Codex npm archive digest mismatch" >&2; exit 1; fi
tar -xzf "$archive" -C "$prefix" "package/vendor/$triple/bin/codex"
mkdir "$prefix/bin"
mv "$prefix/package/vendor/$triple/bin/codex" "$prefix/bin/codex"
if [ "$(digest "$prefix/bin/codex")" != "$binary_digest" ]; then echo "Codex binary digest mismatch" >&2; exit 1; fi
rm "$archive"
if [ "$("$prefix/bin/codex" --version)" != "codex-cli $version" ]; then echo "Codex version mismatch" >&2; exit 1; fi
printf '%s\n' "$prefix/bin/codex"
