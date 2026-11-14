#!/usr/bin/env bash
# Run bounded subscriber API comparisons on isolated, disposable databases.
set -euo pipefail
cd "$(dirname "$0")/.."
# Differential comparisons intentionally enable Novu's unsafe ID-reuse behavior.
export NOVU_ALLOW_RECREATED_SUBSCRIBER=${NOVU_ALLOW_RECREATED_SUBSCRIBER:-true}
export NOVU_POSTGRES_ALLOW_INSECURE=true
reference_source=${NOVU_REFERENCE_SOURCE:?Set NOVU_REFERENCE_SOURCE to the built original Novu checkout}
case "$reference_source" in /*) ;; *) echo 'NOVU_REFERENCE_SOURCE must be absolute' >&2; exit 2;; esac
if [[ ! -f "$reference_source/apps/api/dist/main.js" ]]; then echo 'Reference API build is missing' >&2; exit 2; fi
suffix="$$-$RANDOM"
network="swallow-subscriber-net-$suffix"
mongo="swallow-subscriber-mongo-$suffix"
redis="swallow-subscriber-redis-$suffix"
postgres="swallow-subscriber-pg-$suffix"
reference="swallow-subscriber-api-$suffix"
reference_worker="swallow-subscriber-worker-$suffix"
rust_worker_pid=''
rust_pid=''
rust_ws_pid=''
reference_ws_pid=''
secret_file=''
cleanup() {
  if [[ -n "$secret_file" ]]; then rm -f "$secret_file"; fi
  if [[ -n "$rust_ws_pid" ]]; then kill "$rust_ws_pid" >/dev/null 2>&1 || true; wait "$rust_ws_pid" 2>/dev/null || true; fi
  if [[ -n "$reference_ws_pid" ]]; then kill "$reference_ws_pid" >/dev/null 2>&1 || true; wait "$reference_ws_pid" 2>/dev/null || true; fi
  if [[ -n "$rust_worker_pid" ]]; then kill -CONT "$rust_worker_pid" >/dev/null 2>&1 || true; kill "$rust_worker_pid" >/dev/null 2>&1 || true; wait "$rust_worker_pid" 2>/dev/null || true; fi
  if [[ -n "$rust_pid" ]]; then kill "$rust_pid" >/dev/null 2>&1 || true; wait "$rust_pid" 2>/dev/null || true; fi
  docker rm -f "$reference_worker" "$reference" "$mongo" "$redis" "$postgres" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$network" >/dev/null
if [[ "${NOVU_TEST_MONGO_REPLICA:-0}" == 1 ]]; then
  docker run -d --rm --name "$mongo" --network "$network" mongo:8 --quiet --replSet rs0 --bind_ip_all --setParameter enableTestCommands=1 >/dev/null
else
  docker run -d --rm --name "$mongo" --network "$network" mongo:8 --quiet >/dev/null
fi
docker run -d --rm --name "$redis" --network "$network" redis:7-alpine >/dev/null
docker run -d --rm --name "$postgres" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_compat -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$postgres" pg_isready -q -U postgres && docker exec "$mongo" mongosh --quiet --eval 'db.adminCommand({ping:1}).ok' >/dev/null 2>&1; then break; fi
  if (( i == 59 )); then echo 'Databases did not become ready' >&2; exit 1; fi
  sleep 1
done
if [[ "${NOVU_TEST_MONGO_REPLICA:-0}" == 1 ]]; then
  docker exec "$mongo" mongosh --quiet --eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'$mongo:27017'}]})" >/dev/null
  for ((i=0;i<60;i++)); do
    if [[ $(docker exec "$mongo" mongosh --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null) == true ]]; then break; fi
    if (( i == 59 )); then echo 'MongoDB replica set did not elect a primary' >&2; exit 1; fi
    sleep 1
  done
fi
root=../..
for file in migrations/*.sql "$root"/worker/rust-queue/migrations/*.sql; do
  docker exec -i "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat < "$file" >/dev/null
done
if [[ "${NOVU_TEST_WS_DELIVERY:-0}" == 1 ]]; then
  for file in "$root"/ws/rust-ws/migrations/*.sql; do
    docker exec -i "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat < "$file" >/dev/null
  done
fi
docker exec "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat -c "INSERT INTO novu_rust_users VALUES ('64a000000000000000000001'); INSERT INTO novu_rust_environments VALUES ('64a000000000000000000002','64a000000000000000000003'); INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','64a000000000000000000002','64a000000000000000000001'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile,created_at,updated_at) VALUES ('64a000000000000000000002','alice','64a000000000000000000010','{\"firstName\":\"Jane\",\"phone\":\"+123\",\"avatar\":\"old\",\"data\":{\"old\":true}}','2024-01-01T00:00:00Z','2024-01-01T00:00:00Z');" >/dev/null
docker exec "$mongo" mongosh --quiet --eval 'const d=db.getSiblingDB("novu-db"), u=ObjectId("64a000000000000000000001"), e=ObjectId("64a000000000000000000002"), o=ObjectId("64a000000000000000000003"); d.users.insertOne({_id:u,email:"compat@example.test",firstName:"Test",lastName:"User"}); d.organizations.insertOne({_id:o,name:"Compatibility",branding:{}}); d.environments.insertOne({_id:e,_organizationId:o,name:"Test",identifier:"test",type:"development",apiKeys:[{hash:"4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe",_userId:u}]}); d.subscribers.insertOne({_id:ObjectId("64a000000000000000000010"),_environmentId:e,_organizationId:o,subscriberId:"alice",firstName:"Jane",phone:"+123",avatar:"old",data:{old:true},createdAt:new Date("2024-01-01T00:00:00Z"),updatedAt:new Date("2024-01-01T00:00:00Z"),deleted:false,channels:[],isOnline:false,lastOnlineAt:null});' >/dev/null
if [[ "${NOVU_TEST_MONGO_REPLICA:-0}" == 1 ]]; then
  docker exec "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat -c "INSERT INTO novu_rust_environments VALUES ('64a000000000000000000012','64a000000000000000000013'); INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ('8d83535ce01b1b526ed58628d8668c3566114e20b14340870d0f276bc11916ed','64a000000000000000000012','64a000000000000000000001');" >/dev/null
  docker exec "$mongo" mongosh --quiet --eval 'const d=db.getSiblingDB("novu-db"),u=ObjectId("64a000000000000000000001"),e=ObjectId("64a000000000000000000012"),o=ObjectId("64a000000000000000000013"); d.organizations.insertOne({_id:o,name:"Second compatibility environment",branding:{}}); d.environments.insertOne({_id:e,_organizationId:o,name:"Second",identifier:"second",type:"development",apiKeys:[{hash:"8d83535ce01b1b526ed58628d8668c3566114e20b14340870d0f276bc11916ed",_userId:u}]});' >/dev/null
fi

docker run -d --name "$reference" --network "$network" -p 127.0.0.1::3000 -v "$reference_source:/app:ro" -w /app/apps/api -e NODE_ENV=local -e PORT=3000 -e LOG_LEVEL=error -e FRONT_BASE_URL=http://localhost:4200 -e API_ROOT_URL=http://localhost:3000 -e REDIS_HOST="$redis" -e REDIS_PORT=6379 -e MONGO_URL="mongodb://$mongo:27017/novu-db" -e JWT_SECRET=local-compat-test-secret -e STORE_ENCRYPTION_KEY=0123456789abcdef0123456789abcdef -e NOVU_SECRET_KEY=local-fixture-secret -e IS_SELF_HOSTED=true -e NOVU_ENTERPRISE=false -e NEW_RELIC_ENABLED=false node:22.23.4-alpine3.23 node dist/main.js >/dev/null
reference_port=$(docker port "$reference" 3000/tcp); reference_port=${reference_port##*:}
postgres_port=$(docker port "$postgres" 5432/tcp); postgres_port=${postgres_port##*:}
rust_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-create-target}
cargo build --locked --offline >/dev/null
secret_file=$(mktemp)
chmod 600 "$secret_file"
printf %s local-compat-test-secret > "$secret_file"
env -u JWT_SECRET JWT_SECRET_FILE="$secret_file" DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat" LISTEN_ADDR="127.0.0.1:$rust_port" "$CARGO_TARGET_DIR/debug/novu-rust-api" >/dev/null 2>&1 &
rust_pid=$!
for ((i=0;i<90;i++)); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: ApiKey compat-key' "http://127.0.0.1:$reference_port/v2/subscribers/alice" || true)
  rust_code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: ApiKey compat-key' "http://127.0.0.1:$rust_port/v2/subscribers/alice" || true)
  if [[ "$code" == 200 && "$rust_code" == 200 ]]; then break; fi
  if ! kill -0 "$rust_pid" 2>/dev/null || [[ $(docker inspect -f '{{.State.Running}}' "$reference") != true ]] || (( i == 89 )); then
    echo "API startup failed (reference=$code rust=$rust_code)" >&2
    docker logs "$reference" 2>&1 | tail -40 >&2
    exit 1
  fi
  sleep 1
done
export NOVU_REFERENCE_URL="http://127.0.0.1:$reference_port" NOVU_RUST_URL="http://127.0.0.1:$rust_port" NOVU_TEST_API_KEY=compat-key
export NOVU_RUST_PG_CONTAINER="$postgres" NOVU_REFERENCE_MONGO_CONTAINER="$mongo" NOVU_REFERENCE_REDIS_CONTAINER="$redis"
if [[ "${NOVU_TEST_ACTION_ONLY:-0}" == 1 ]]; then
  python3 tests/reference_notification_action_compare.py
  exit 0
fi
if [[ "${NOVU_TEST_INAPP_IDENTITY_ONLY:-0}" == 1 ]]; then
  python3 tests/reference_in_app_identity_compare.py
  exit 0
fi
if [[ "${NOVU_TEST_BULK_CONTEXT_ONLY:-0}" == 1 ]]; then
  export NOVU_TEST_ENVIRONMENT_ID=64a000000000000000000002
  python3 tests/reference_notification_bulk_compare.py
  exit 0
fi
if [[ "${NOVU_TEST_SEVERITY_ONLY:-0}" == 1 ]]; then
  python3 tests/reference_count_severity_compare.py
  exit 0
fi
if [[ "${NOVU_TEST_SNOOZE_ONLY:-0}" != 1 ]]; then
python3 tests/reference_subscriber_create_compare.py
python3 tests/reference_subscriber_patch_compare.py
python3 tests/reference_subscriber_list_compare.py
python3 tests/reference_subscriber_online_compare.py
python3 tests/reference_topics_compare.py
python3 tests/reference_topic_multiple_subscriptions_compare.py
python3 tests/reference_subscription_create_compare.py
python3 tests/reference_subscription_list_compare.py
python3 tests/reference_subscription_update_compare.py
if [[ "${NOVU_TEST_MONGO_REPLICA:-0}" == 1 ]]; then
  export NOVU_SECOND_TEST_API_KEY=compat-second-key NOVU_TEST_ENVIRONMENT_ID=64a000000000000000000002
  python3 tests/reference_subscriber_delete_compare.py
  python3 tests/reference_subscription_delete_compare.py
  python3 tests/reference_subscription_preference_lifecycle_compare.py
fi
python3 tests/reference_topics_v2_compare.py
python3 tests/reference_topics_v2_list_compare.py
fi
# A single in-app workflow without a workflow preference resource is a bounded fixture.
docker exec "$mongo" mongosh --quiet --eval 'const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000004"),m=ObjectId("64a000000000000000000005"); d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"in_app",active:true,content:"hello",deleted:false}); d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Greeting",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"greeting"}],steps:[{active:true,_templateId:m}],createdAt:new Date("2024-01-01T00:00:00Z"),updatedAt:new Date("2024-01-01T00:00:00Z")});' >/dev/null
docker exec "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat -c "INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content) VALUES ('64a000000000000000000002','greeting',true,true,'hello')" >/dev/null
if [[ "${NOVU_TEST_SNOOZE_ONLY:-0}" != 1 ]]; then python3 tests/reference_global_preferences_compare.py; fi

if [[ "${NOVU_TEST_PREFERENCE_DELIVERY:-0}" == 1 ]]; then
  # Explicit workflow resource preferences; original per-workflow records are
  # inserted after API-only comparison so they do not change its fixture.
  docker exec "$mongo" mongosh --quiet --eval 'const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000004"),m=ObjectId("64a000000000000000000005"),u=ObjectId("64a000000000000000000006"); d.integrations.insertOne({_environmentId:e,_organizationId:o,providerId:"novu",channel:"in_app",active:true,primary:true,credentials:{hmac:false},deleted:false}); d.notificationtemplates.insertOne({_id:u,_environmentId:e,_organizationId:o,name:"Urgent",active:true,draft:false,critical:true,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"urgent"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()}); d.preferences.insertMany([{_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false},{_environmentId:e,_organizationId:o,_templateId:u,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:true},channels:{in_app:{enabled:true}}},deleted:false}]);' >/dev/null
  docker exec "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat -c "UPDATE novu_rust_workflows SET inbox_metadata='{\"id\":\"64a000000000000000000004\",\"name\":\"Greeting\",\"slug\":\"greeting_wf_eV5Qd9HRCowdmEZY\",\"critical\":false,\"updatedAt\":\"2024-01-01T00:00:00.000Z\"}'::jsonb WHERE environment_id='64a000000000000000000002' AND identifier='greeting'; INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES ('64a000000000000000000002','urgent',true,true,'hello','{\"id\":\"64a000000000000000000006\",\"name\":\"Urgent\",\"critical\":true,\"tags\":[]}'::jsonb); INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,in_app_enabled) VALUES ('64a000000000000000000002','greeting',true,false,true),('64a000000000000000000002','urgent',true,true,true);" >/dev/null
  if [[ "${NOVU_TEST_SNOOZE_ONLY:-0}" != 1 ]]; then
    python3 tests/reference_subscription_preference_api_compare.py
    python3 tests/reference_subscriber_subscriptions_compare.py
  fi
  worker_options=()
  if [[ "${NOVU_TEST_SLACK_ONLY:-0}" == 1 ]]; then
    worker_options=(-v "$PWD/tests/reference_slack_preload.cjs:/novu-slack-preload.cjs:ro" -e NODE_OPTIONS=--require=/novu-slack-preload.cjs)
  fi
  if [[ "${NOVU_TEST_RESEND_ONLY:-0}" == 1 ]]; then
    worker_options=(-v "$PWD/tests/reference_resend_preload.cjs:/novu-resend-preload.cjs:ro" -e NODE_OPTIONS=--require=/novu-resend-preload.cjs)
  fi
  if [[ "${NOVU_TEST_EMAIL_ONLY:-0}" == 1 || "${NOVU_TEST_EMAIL_QUEUED_ONLY:-0}" == 1 ]]; then
    worker_options=(-v "$PWD/tests/reference_email_preload.cjs:/novu-email-preload.cjs:ro" -e NODE_OPTIONS=--require=/novu-email-preload.cjs)
  fi
  if [[ "${NOVU_TEST_SMS_ONLY:-0}" == 1 || "${NOVU_TEST_SMS_QUEUED_ONLY:-0}" == 1 ]]; then
    worker_options=(-v "$PWD/tests/reference_sms_preload.cjs:/novu-sms-preload.cjs:ro" -e NODE_OPTIONS=--require=/novu-sms-preload.cjs)
  fi
  if [[ "${NOVU_TEST_PUSH_ONLY:-0}" == 1 || "${NOVU_TEST_PUSH_QUEUED_ONLY:-0}" == 1 || "${NOVU_TEST_PUSH_RACE_ONLY:-0}" == 1 || "${NOVU_TEST_FCM_MULTI_ONLY:-0}" == 1 ]]; then
    worker_options=(-v "$PWD/tests/reference_push_preload.cjs:/novu-push-preload.cjs:ro" -e NODE_OPTIONS=--require=/novu-push-preload.cjs)
  fi
  docker run -d --name "$reference_worker" --network "$network" -v "$reference_source:/app:ro" ${worker_options[@]+"${worker_options[@]}"} -w /app/apps/worker -e NODE_ENV=ci -e PORT=3001 -e LOG_LEVEL=error -e API_ROOT_URL="http://$reference:3000" -e REDIS_HOST="$redis" -e REDIS_PORT=6379 -e REDIS_DB_INDEX=0 -e MONGO_URL="mongodb://$mongo:27017/novu-db" -e JWT_SECRET=local-compat-test-secret -e STORE_NOTIFICATION_CONTENT="${NOVU_TEST_STORE_CONTENT:-false}" -e IS_EXPIRED_TOKENS_REMOVAL_ENABLED="${IS_EXPIRED_TOKENS_REMOVAL_ENABLED:-false}" -e STORE_ENCRYPTION_KEY=0123456789abcdef0123456789abcdef -e NOVU_SECRET_KEY=local-fixture-secret -e IS_SELF_HOSTED=true -e NOVU_ENTERPRISE=false -e NEW_RELIC_ENABLED=false node:22.23.4-alpine3.23 node dist/main.js >/dev/null
  cargo build --locked --offline --manifest-path "$root/worker/rust-queue/Cargo.toml" --bin in-app-worker >/dev/null
  DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat" "$CARGO_TARGET_DIR/debug/in-app-worker" >/dev/null 2>&1 &
  rust_worker_pid=$!
  export NOVU_REFERENCE_MONGO_CONTAINER="$mongo" NOVU_RUST_PG_CONTAINER="$postgres" NOVU_REFERENCE_WORKER_CONTAINER="$reference_worker" NOVU_RUST_WORKER_PID="$rust_worker_pid"
  if [[ "${NOVU_TEST_SLACK_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_slack_workflow_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_FCM_MULTI_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_fcm_multi_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_PUSH_RACE_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_push_race_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_PUSH_QUEUED_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_push_queued_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_PUSH_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    export STORE_NOTIFICATION_CONTENT="${NOVU_TEST_STORE_CONTENT:-false}"
    python3 tests/reference_push_workflow_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_SMS_QUEUED_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_sms_queued_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_SMS_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    export STORE_NOTIFICATION_CONTENT="${NOVU_TEST_STORE_CONTENT:-false}"
    python3 tests/reference_sms_workflow_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_EMAIL_QUEUED_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_email_queued_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_RESEND_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_resend_workflow_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_EMAIL_ONLY:-0}" == 1 ]]; then
    export TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat"
    python3 tests/reference_email_workflow_compare.py
    exit 0
  fi
  if [[ "${NOVU_TEST_SNOOZE_ONLY:-0}" != 1 ]]; then
    python3 tests/reference_workflow_preferences_compare.py
    python3 tests/reference_preference_delivery_compare.py
    python3 tests/reference_topic_delivery_compare.py
    python3 tests/reference_topic_subscription_preference_delivery_compare.py
  fi
  # DTO metadata is needed for the worker-produced notification list.
  docker exec "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat -c "UPDATE novu_rust_workflows SET inbox_metadata=jsonb_build_object('id','64a000000000000000000004','identifier','greeting','name','Greeting','critical',false,'tags',jsonb_build_array(),'severity','none','payloadAsData',false) WHERE environment_id='64a000000000000000000002' AND identifier='greeting'" >/dev/null
  python3 tests/reference_snoozed_read_compare.py
  if [[ "${NOVU_TEST_SEVERITY_QUEUE:-0}" == 1 ]]; then
    python3 tests/reference_queued_severity_compare.py
  fi
  if [[ "${NOVU_TEST_WS_DELIVERY:-0}" == 1 ]]; then
    rust_ws_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
    reference_ws_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
    cargo build --locked --offline --manifest-path "$root/ws/rust-ws/Cargo.toml" >/dev/null
    DATABASE_URL="postgresql://postgres@127.0.0.1:$postgres_port/novu_compat" LISTEN_ADDR="127.0.0.1:$rust_ws_port" JWT_SECRET=local-compat-test-secret "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & rust_ws_pid=$!
    NOVU_REFERENCE_SOURCE="$reference_source" REF_WS_PORT="$reference_ws_port" JWT_SECRET=local-compat-test-secret node "$root/ws/rust-ws/tests/reference_gateway.cjs" >/dev/null 2>&1 & reference_ws_pid=$!
    for ((i=0;i<30;i++)); do
      if curl -fsS "http://127.0.0.1:$reference_ws_port/status" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$rust_ws_port/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1; then break; fi
      if ! kill -0 "$rust_ws_pid" 2>/dev/null || ! kill -0 "$reference_ws_pid" 2>/dev/null || (( i == 29 )); then echo 'WS startup failed' >&2; exit 1; fi
      sleep 1
    done
    docker exec "$postgres" psql -v ON_ERROR_STOP=1 -U postgres -d novu_compat -c "INSERT INTO novu_rust_inbox_environments(environment_id,application_identifier,in_app_active,hmac_required,is_development,remove_novu_branding,max_snooze_duration_hours) VALUES ('64a000000000000000000002','compat-ws-app',true,false,true,false,24) ON CONFLICT (environment_id) DO UPDATE SET application_identifier=EXCLUDED.application_identifier" >/dev/null
    NOVU_REFERENCE_SOURCE="$reference_source" REF_WS_URL="http://127.0.0.1:$reference_ws_port" RUST_WS_URL="http://127.0.0.1:$rust_ws_port" node "$root/ws/rust-ws/tests/worker_delivery.cjs"
  fi
fi
