# vsr

Viewstamped Replication (Revisited) in Go.

## Usage

```sh
make check          # fmt, vet, test
make build          # binaries in build/bin
make demo           # local SQLite cluster with load, chaos, and dashboard
make up             # docker compose cluster
make vsrctl ARGS='get a'
make e2e            # Maelstrom lin-kv against the compose cluster
make chaos          # chaos against the compose cluster
make help           # all targets
```
