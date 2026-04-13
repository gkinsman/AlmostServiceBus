#!/bin/bash
# Compare lock-renewal behavior between Microsoft's official ASB emulator and ours.
#
# Requirements on the host: Docker (for MS emulator), .NET 10 SDK, jq.
#
# Usage:  ./run-lock-renewal-comparison.sh
#
# Result: prints per-test pass/fail for each emulator side-by-side. Exits
# non-zero if any test diverges.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
HARNESS_DIR="$SCRIPT_DIR/LockRenewalComparison"
RESULTS_DIR="$SCRIPT_DIR/comparison-results"
mkdir -p "$RESULTS_DIR"

run_tests() {
    local label="$1"
    local conn="$2"
    local trx="$RESULTS_DIR/${label}.trx"
    local log="$RESULTS_DIR/${label}.log"

    echo "=== Running lock-renewal tests against: $label ==="
    rm -f "$trx"
    SBE_CONNECTION_STRING="$conn" dotnet test "$HARNESS_DIR" \
        --no-build \
        --logger "trx;LogFileName=$trx" \
        --verbosity normal \
        > "$log" 2>&1 || true
    echo "  Log: $log"
    echo "  Trx: $trx"
}

summarize() {
    local label="$1"
    local trx="$RESULTS_DIR/${label}.trx"
    if [ ! -f "$trx" ]; then
        echo "  (no trx file — test run failed to start)"
        return
    fi
    # Extract test results from the trx XML.
    grep -oP 'outcome="[^"]*"\s+testName="[^"]*"' "$trx" \
        | sed -E 's/outcome="([^"]+)"\s+testName="[^"]+\.([^"]+)"/\1 \2/' \
        | sort -u
}

cd "$REPO_ROOT"

# --- Build the harness once ---
echo "=== Building harness ==="
dotnet build "$HARNESS_DIR" --verbosity quiet || {
    echo "Harness build failed" >&2
    exit 1
}

# ------------------------------------------------------------------ MS emulator
echo ""
echo "============================================================"
echo "PHASE 1: Microsoft official ASB emulator (Docker)"
echo "============================================================"

cd "$SCRIPT_DIR"
# Copy our Config.json into place so the MS emulator has the queues we need.
if [ ! -f Config.json ]; then
    cp "$HARNESS_DIR/Config.json" Config.json
fi

# Start the MS emulator. Override docker-compose to mount the config file.
cat > docker-compose.override.yml <<'EOF'
services:
  servicebus-emulator:
    volumes:
      - ./Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro
    environment:
      CONFIG_PATH: /ServiceBus_Emulator/ConfigFiles/Config.json
EOF

docker compose down -v 2>/dev/null || true
docker compose up -d
echo "Waiting for MS emulator to be healthy..."
MS_READY=0
for i in $(seq 1 60); do
    if docker compose exec -T servicebus-emulator curl -sf http://localhost:5300/ > /dev/null 2>&1; then
        MS_READY=1
        echo "MS emulator ready!"
        break
    fi
    sleep 2
done

if [ "$MS_READY" = "1" ]; then
    # The MS emulator exposes SAS creds as env vars inside the container; the
    # public documented default is below.
    MS_CONN="Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"
    cd "$REPO_ROOT"
    run_tests "ms-emulator" "$MS_CONN"
else
    echo "WARNING: MS emulator failed to become ready — skipping." >&2
    echo "(Check 'docker compose logs' in $SCRIPT_DIR for details.)" >&2
fi

cd "$SCRIPT_DIR"
docker compose down -v 2>/dev/null || true
rm -f docker-compose.override.yml Config.json

# ------------------------------------------------------------------ Our emulator
echo ""
echo "============================================================"
echo "PHASE 2: Our AlmostServiceBus emulator"
echo "============================================================"

# Kill anything on our ports first.
for port in 5672 5671 5300 443 15672; do
    fuser -k "$port/tcp" 2>/dev/null || true
done
sleep 1

# Start our emulator in the background.
cd "$REPO_ROOT"
OUR_LOG="$RESULTS_DIR/our-emulator-runtime.log"
dotnet run --project src/AlmostServiceBus.Host --no-build > "$OUR_LOG" 2>&1 &
OUR_PID=$!
echo "Started emulator (PID $OUR_PID); log: $OUR_LOG"

echo "Waiting for our emulator to be ready..."
OUR_READY=0
for i in $(seq 1 30); do
    if curl -sf http://localhost:5300/ > /dev/null 2>&1 || nc -z localhost 5672 2>/dev/null; then
        OUR_READY=1
        echo "Our emulator ready!"
        break
    fi
    sleep 1
done

if [ "$OUR_READY" = "1" ]; then
    # Our emulator auto-creates queues via AMQP attach, but we need to be sure
    # the session queue has RequiresSession=true. Do that via the management
    # API before running the harness.
    curl -s -X PUT http://localhost:5300/lock-renewal-queue \
        -H "Content-Type: application/atom+xml;type=entry;charset=utf-8" \
        -d '<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT10S</LockDuration><RequiresSession>false</RequiresSession></QueueDescription></content></entry>' \
        > /dev/null || true

    curl -s -X PUT http://localhost:5300/session-renewal-queue \
        -H "Content-Type: application/atom+xml;type=entry;charset=utf-8" \
        -d '<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><LockDuration>PT10S</LockDuration><RequiresSession>true</RequiresSession></QueueDescription></content></entry>' \
        > /dev/null || true

    OUR_CONN="Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator"
    run_tests "our-emulator" "$OUR_CONN"
else
    echo "WARNING: Our emulator failed to become ready — skipping." >&2
fi

kill "$OUR_PID" 2>/dev/null || true
wait "$OUR_PID" 2>/dev/null || true

# ------------------------------------------------------------------ Compare
echo ""
echo "============================================================"
echo "RESULTS"
echo "============================================================"

MS_SUMMARY="$RESULTS_DIR/ms-summary.txt"
OUR_SUMMARY="$RESULTS_DIR/our-summary.txt"
summarize ms-emulator  > "$MS_SUMMARY"
summarize our-emulator > "$OUR_SUMMARY"

printf "\n%-60s  %-15s  %-15s\n" "TEST" "MS-EMULATOR" "OURS"
printf -- "--------------------------------------------------------------------------------------\n"

# Join the two summaries by test name.
join -j 2 -a 1 -a 2 -e "MISSING" -o '0,1.1,2.1' \
    <(sort -k2 "$MS_SUMMARY") \
    <(sort -k2 "$OUR_SUMMARY") \
    | awk '{ printf "%-60s  %-15s  %-15s\n", $1, $2, $3 }'

# Diff-style summary of divergences.
echo ""
echo "============================================================"
echo "DIVERGENCES (tests where MS and ours disagree)"
echo "============================================================"
divergences=$(
    join -j 2 -a 1 -a 2 -e "MISSING" -o '0,1.1,2.1' \
        <(sort -k2 "$MS_SUMMARY") \
        <(sort -k2 "$OUR_SUMMARY") \
        | awk '$2 != $3 { print }'
)
if [ -z "$divergences" ]; then
    echo "(none — we're 1:1 compatible for lock renewal!)"
    exit 0
else
    echo "$divergences"
    exit 1
fi
