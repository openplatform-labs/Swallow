#!/usr/bin/env bash
# Creates disposable TLS and plaintext databases and an ephemeral local CA.
set -euo pipefail
cd "$(dirname "$0")/.."
dir=$(mktemp -d "${TMPDIR:-/tmp}/novu-pg-tls.XXXXXX")
tls_db="novu-pg-tls-$$"
plain_db="novu-pg-plain-$$"
api_pid=''
cleanup() { if [[ -n "$api_pid" ]]; then kill "$api_pid" >/dev/null 2>&1 || true; wait "$api_pid" 2>/dev/null || true; fi; docker rm -f "$tls_db" "$plain_db" >/dev/null 2>&1 || true; rm -rf "$dir"; }
trap cleanup EXIT
chmod 755 "$dir"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=Swallow test CA' -keyout "$dir/ca.key" -out "$dir/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj '/CN=localhost' -keyout "$dir/server.key" -out "$dir/server.csr" >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' > "$dir/extensions.cnf"
openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial -days 1 -extfile "$dir/extensions.cnf" -out "$dir/server.crt" >/dev/null 2>&1
chmod 644 "$dir/server.key" "$dir/server.crt" "$dir/ca.crt"
docker run -d --rm --name "$tls_db" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_tls -p 127.0.0.1::5432 -v "$dir:/fixture:ro" postgres:17-alpine sh -c 'cp /fixture/server.key /tmp/server.key; cp /fixture/server.crt /tmp/server.crt; chown postgres:postgres /tmp/server.key /tmp/server.crt; chmod 600 /tmp/server.key; exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key' >/dev/null
docker run -d --rm --name "$plain_db" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_tls -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$tls_db" pg_isready -q -U postgres && docker exec "$plain_db" pg_isready -q -U postgres; then break; fi
  if (( i == 59 )); then echo 'PostgreSQL TLS fixtures did not become ready' >&2; exit 1; fi
  sleep 1
done
tls_port=$(docker port "$tls_db" 5432/tcp); tls_port=${tls_port##*:}
plain_port=$(docker port "$plain_db" 5432/tcp); plain_port=${plain_port##*:}
env -u NOVU_POSTGRES_ALLOW_INSECURE NOVU_POSTGRES_CA_FILE="$dir/ca.crt" TEST_DATABASE_URL="postgresql://postgres@localhost:$tls_port/novu_tls?sslmode=disable" TEST_POSTGRES_TLS_EXPECT=tls cargo test --locked --test postgres_tls -- --ignored
env -u NOVU_POSTGRES_ALLOW_INSECURE -u NOVU_POSTGRES_CA_FILE TEST_DATABASE_URL="postgresql://postgres@localhost:$tls_port/novu_tls" TEST_POSTGRES_TLS_EXPECT=reject cargo test --locked --test postgres_tls -- --ignored
env -u NOVU_POSTGRES_ALLOW_INSECURE NOVU_POSTGRES_CA_FILE="$dir/ca.crt" TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$tls_port/novu_tls" TEST_POSTGRES_TLS_EXPECT=reject cargo test --locked --test postgres_tls -- --ignored
env -u NOVU_POSTGRES_ALLOW_INSECURE -u NOVU_POSTGRES_CA_FILE TEST_DATABASE_URL="postgresql://postgres@localhost:$plain_port/novu_tls" TEST_POSTGRES_TLS_EXPECT=reject cargo test --locked --test postgres_tls -- --ignored
NOVU_POSTGRES_ALLOW_INSECURE=true TEST_DATABASE_URL="postgresql://postgres@localhost:$plain_port/novu_tls" TEST_POSTGRES_TLS_EXPECT=plain cargo test --locked --test postgres_tls -- --ignored
cargo build --locked --bin novu-rust-api >/dev/null
api_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
env -u NOVU_POSTGRES_ALLOW_INSECURE NOVU_POSTGRES_CA_FILE="$dir/ca.crt" DATABASE_URL="postgresql://postgres@localhost:$tls_port/novu_tls" LISTEN_ADDR="127.0.0.1:$api_port" "${CARGO_TARGET_DIR:-target}/debug/novu-rust-api" >"$dir/api.log" 2>&1 & api_pid=$!
for ((i=0;i<30;i++)); do
  status=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$api_port/v2/subscribers/missing" || true)
  if [[ "$status" == 401 ]]; then break; fi
  if (( i == 29 )); then echo 'API failed to start with verified PostgreSQL TLS' >&2; cat "$dir/api.log" >&2; exit 1; fi
  sleep 1
done
echo 'API started using verified PostgreSQL TLS'
