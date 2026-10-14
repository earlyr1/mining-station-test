#!/usr/bin/env bash
# Однократная настройка Redpanda (§2.2, §2.3): параметры кластера и топики.
# Идемпотентен: создаёт только отсутствующие топики, затем сверяет партиции, реплики
# и настройки каждого топика и падает при расхождении (лечится `make reset`).
set -euo pipefail

BROKERS="${VQ_RPK_BROKERS:-redpanda:9092}"
ADMIN="${VQ_RPK_ADMIN:-redpanda:9644}"
REPLICAS=1

# Топики §2.3: «имя|партиции|настройки через пробел».
TOPICS=(
  "telemetry.v1|1|cleanup.policy=delete retention.ms=604800000"
  "queue.v1|1|cleanup.policy=compact min.compaction.lag.ms=86400000"
  "decision.v1|1|cleanup.policy=delete retention.ms=604800000"
)

rpk_() { rpk -X "brokers=${BROKERS}" -X "admin.hosts=${ADMIN}" "$@"; }
log() { echo "redpanda-init: $*"; }
die() {
  echo "redpanda-init: ОШИБКА: $*" >&2
  exit 1
}

# Брокер уже healthy (depends_on), но Kafka API и admin API ждём явно — до 30 с.
for _ in $(seq 1 30); do
  if rpk_ cluster info >/dev/null 2>&1 && rpk_ cluster config get auto_create_topics_enabled >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
rpk_ cluster info >/dev/null || die "брокер ${BROKERS} недоступен"

# 1. Параметры кластера: без автосоздания топиков, первый join группы без задержки.
set_cluster() {
  local key="$1" want="$2" got
  rpk_ cluster config set "$key" "$want" >/dev/null
  got="$(rpk_ cluster config get "$key")"
  [[ "$got" == "$want" ]] || die "параметр кластера $key = '$got', ожидалось '$want'"
  log "$key = $got"
}
set_cluster auto_create_topics_enabled false
set_cluster group_initial_rebalance_delay 0

# 2. Создать только отсутствующие топики.
existing="$(rpk_ topic list | awk 'NR > 1 {print $1}')"
for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name parts configs <<<"$spec"
  if grep -qxF -- "$name" <<<"$existing"; then
    log "топик $name уже есть, не создаю"
    continue
  fi
  args=()
  for kv in $configs; do args+=(-c "$kv"); done
  rpk_ topic create "$name" -p "$parts" -r "$REPLICAS" "${args[@]}" >/dev/null
  log "топик $name создан"
done

# 3. Сверить партиции, реплики и настройки с §2.3.
for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name parts configs <<<"$spec"
  summary="$(rpk_ topic describe "$name" -s)"
  got_parts="$(awk '$1 == "PARTITIONS" {print $2}' <<<"$summary")"
  got_repl="$(awk '$1 == "REPLICAS" {print $2}' <<<"$summary")"
  [[ "$got_parts" == "$parts" ]] || die "$name: партиций '$got_parts', ожидалось $parts — нужен make reset"
  [[ "$got_repl" == "$REPLICAS" ]] || die "$name: реплик '$got_repl', ожидалось $REPLICAS — нужен make reset"
  cfg="$(rpk_ topic describe "$name" -c)"
  for kv in $configs; do
    key="${kv%%=*}"
    want="${kv#*=}"
    got="$(awk -v k="$key" '$1 == k {print $2}' <<<"$cfg")"
    [[ "$got" == "$want" ]] || die "$name: $key = '$got', ожидалось '$want' — нужен make reset"
  done
  log "топик $name сверен: партиций $got_parts, реплик $got_repl, $configs"
done
log "готово"
