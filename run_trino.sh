#!/bin/bash
set -e

# --- Cleanup Function ---
cleanup() {
    echo "Cleaning up..."
    # Find process on port 8080. Allow failure if no process found.
    PID=$(lsof -ti :8080 || true)
    if [ ! -z "$PID" ]; then
        echo "Killing existing process on port 8080 (PID: $PID)..."
        kill -9 $PID || true
    fi
}

# Run cleanup before starting
cleanup

# --- Docker Check ---
check_docker() {
    docker info > /dev/null 2>&1
}

if ! check_docker; then
    echo "Docker is not running. Attempting to start Docker..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open -a Docker
    else
        echo "Cannot start Docker automatically on this OS. Please start Docker manually."
        exit 1
    fi
    
    echo "Waiting for Docker to start..."
    while ! check_docker; do
        sleep 5
        echo -n "."
    done
    echo "Docker started!"
fi

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d
echo "Waiting for Docker services to be ready..."
sleep 5 

# --- Configure Iceberg Catalog ---
if [ ! -f testing/trino-server-dev/etc/catalog/iceberg.properties ]; then
    echo "Configuring Iceberg catalog..."
    mkdir -p testing/trino-server-dev/etc/catalog

    cat <<CONFIG > testing/trino-server-dev/etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://localhost:19120/api/v2
iceberg.nessie-catalog.ref=main
iceberg.nessie-catalog.default-warehouse-dir=s3://lake-data

fs.native-s3.enabled=true
s3.endpoint=http://localhost:9000
s3.aws-access-key=admin
s3.aws-secret-key=password
s3.path-style-access=true
s3.region=us-east-1
CONFIG
else
    echo "Iceberg configuration exists. Skipping creation."
fi

# --- Build CLI if needed ---
echo "Checking for Trino CLI..."
if [ ! -f client/trino-cli/target/trino-cli*-executable.jar ]; then
    echo "Trino CLI not found. Building it..."
    ./mvnw install -pl client/trino-cli -am -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all=true -q
fi

# --- Get Classpath ---
echo "Finding classpath (cp.txt)..."
ROOT_DIR=$(pwd)
CP_FILE=""

if [ -f "$ROOT_DIR/cp.txt" ]; then
    CP_FILE="$ROOT_DIR/cp.txt"
    echo "Using existing cp.txt from root."
elif [ -f "$ROOT_DIR/testing/trino-server-dev/cp.txt" ]; then
    CP_FILE="$ROOT_DIR/testing/trino-server-dev/cp.txt"
    echo "Using existing cp.txt from testing/trino-server-dev."
else
    echo "Generating classpath..."
    cd testing/trino-server-dev
    ../../mvnw dependency:build-classpath -Dmdep.outputFile=cp.txt -Dair.check.skip-all=true -DskipTests -q
    CP_FILE="$ROOT_DIR/testing/trino-server-dev/cp.txt"
    cd "$ROOT_DIR"
fi

if [ ! -f "$CP_FILE" ]; then
    echo "Error: Classpath file not found!"
    exit 1
fi

# --- Start Trino ---
echo "Starting Trino Development Server in background..."
cd testing/trino-server-dev
java -cp $(cat "$CP_FILE"):target/classes \
  -ea \
  -Dconfig=etc/config.properties \
  -Dlog.levels-file=etc/log.properties \
  -Djdk.attach.allowAttachSelf=true \
  --sun-misc-unsafe-memory-access=allow \
  --add-modules jdk.incubator.vector \
  io.trino.server.DevelopmentServer &
TRINO_PID=$!

echo "Waiting for Trino to start (PID: $TRINO_PID)..."
# Wait for port 8080 to open
while ! nc -z localhost 8080; do   
  if ! ps -p $TRINO_PID > /dev/null; then
     echo "Trino process died! Check logs below:"
     wait $TRINO_PID || true
     exit 1
  fi
  sleep 1
done
echo "Trino port is open. Waiting for server to be fully ready..."

# Retry loop to check health status
MAX_HEALTH_RETRIES=120
HEALTH_COUNT=0
READY=false

echo "Waiting for server to become ready..."

while [ $HEALTH_COUNT -lt $MAX_HEALTH_RETRIES ]; do
    RESPONSE=$(curl -s http://localhost:8080/v1/info || echo "")
    
    if [[ "$RESPONSE" == *'"starting":false'* ]]; then
        echo "✅ Trino Server is READY!"
        READY=true
        break
    elif [[ "$RESPONSE" == *'"starting":true'* ]]; then
         if [ $((HEALTH_COUNT % 6)) -eq 0 ]; then
             echo "Server is still initializing... ($HEALTH_COUNT/$MAX_HEALTH_RETRIES)"
         fi
    else
        echo "Waiting for server to respond... ($HEALTH_COUNT/$MAX_HEALTH_RETRIES)"
    fi
    
    if ! ps -p $TRINO_PID > /dev/null; then
        echo "Trino process died unexpectedly!"
        exit 1
    fi

    sleep 5
    HEALTH_COUNT=$((HEALTH_COUNT+1))
done

if [ "$READY" = "false" ]; then
    echo "Server failed to become ready in time."
    echo "Last response: $RESPONSE"
    exit 1
fi

# --- Run CLI Command ---
echo "Creating iceberg.test schema..."
CLI_JAR=$(find ../../client/trino-cli/target -name "trino-cli*-executable.jar" | head -n 1)

if [ -z "$CLI_JAR" ]; then
    echo "Error: CLI JAR not found!"
    kill $TRINO_PID
    exit 1
fi

echo "Using CLI: $CLI_JAR"
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"

# Try to create schema
if java $JAVA_OPTS -jar "$CLI_JAR" --server localhost:8080 --execute "CREATE SCHEMA IF NOT EXISTS iceberg.test"; then
    echo "✅ SCHEMA CREATED SUCCESSFULLY!"
else
    echo "❌ Failed to create schema."
fi

echo "Trino is running. PID: $TRINO_PID"
echo "Press Ctrl+C to stop the server."
wait $TRINO_PID
