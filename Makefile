.PHONY: test vet lint native-gate

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

native-gate:
	@test -n "$(SESSIONKIT_CODEX_BIN)" || { echo 'SESSIONKIT_CODEX_BIN must point to a digest-verified binary from scripts/install-codex.sh <version>' >&2; exit 1; }
	SESSIONKIT_CODEX_BIN="$(SESSIONKIT_CODEX_BIN)" go test -count=1 -tags=native -timeout=10m ./native/codex
