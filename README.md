# Confluent Cloud Exporter

A Prometheus exporter for Confluent Cloud that dynamically discovers resources and fetches metrics using the official Confluent Cloud Metrics API.

## Problem Solved

Confluent Cloud provides a rich set of metrics through its API, but there isn't an official, widely adopted Prometheus exporter that handles dynamic resource discovery across an entire organization. This exporter aims to fill that gap by:

1.  **Dynamic Discovery:** Automatically finding Kafka clusters, connectors, schema registries, ksqlDB clusters, and Flink compute pools across specified environments (or all environments).
2.  **Efficient Metrics Fetching:** Using the `/v2/metrics/cloud/export` endpoint to retrieve metrics in Prometheus format efficiently.
3.  **Standard Prometheus Integration:** Exposing metrics in a format easily consumable by Prometheus.

## What it Does

-   **Connects** to the Confluent Cloud Management API using an API key and secret to discover resources.
-   **Connects** to the Confluent Cloud Metrics API (`api.telemetry.confluent.cloud`) using the same credentials.
-   **Periodically refreshes** the list of discovered resources in the background.
-   **Fetches metrics** using the `/v2/metrics/cloud/export` endpoint, requesting Prometheus text format.
-   **Handles API rate limits** using an adaptive rate limiter based on response headers.
-   **Caches** discovered resources and metrics API responses to reduce load and improve performance.
-   **Enriches** metrics with labels derived from discovered resource metadata (e.g., environment name, cluster name).
-   **Exposes** metrics on `/metrics` for Prometheus scraping.
-   **Provides** a `/healthz` endpoint for basic health checks.

## Building

You need Go installed (version 1.24.1 or later recommended).

```bash
# Clone the repository (if you haven't already)
# git clone ...
# cd confluent-cloud-exporter

# Build the binary
go build -o confluent-cloud-exporter .
```

This will create an executable named `confluent-cloud-exporter` in the current directory.

## Running

You can run the exporter directly or via Docker/Kubernetes. Configuration can be provided via command-line flags, environment variables, or a configuration file.

### Configuration Methods

Configuration is loaded with the following precedence (highest first):

1.  **Command-line flags** (e.g., `-confluent.api-key=...`)
2.  **Environment variables** (prefixed with `CONFLUENT_EXPORTER_`, e.g., `CONFLUENT_EXPORTER_API_KEY=...`)
3.  **Configuration file** (specified by `-config.file=path/to/config.yaml`)
4.  **Default values**

### Configuration Parameters

| Parameter              | Flag                          | Environment Variable                 | Config File Key        | Default        | Description                                                                 |
| :--------------------- | :---------------------------- | :----------------------------------- | :--------------------- | :------------- | :-------------------------------------------------------------------------- |
| API Key                | `-confluent.api-key`          | `CONFLUENT_EXPORTER_API_KEY`         | `apiKey`               | *(required)*   | Your Confluent Cloud API Key.                                               |
| API Secret             | `-confluent.api-secret`       | `CONFLUENT_EXPORTER_API_SECRET`      | `apiSecret`            | *(required)*   | Your Confluent Cloud API Secret.                                            |
| Listen Address         | `-web.listen-address`         | `CONFLUENT_EXPORTER_LISTEN_ADDRESS`  | `listenAddress`        | `:9184`        | Address and port for the exporter's HTTP server.                            |
| Log Level              | `-log.level`                  | `CONFLUENT_EXPORTER_LOG_LEVEL`       | `logLevel`             | `info`         | Logging level (`debug`, `info`, `warn`, `error`).                           |
| Discovery Interval     | `-discovery.interval`         | `CONFLUENT_EXPORTER_DISCOVERY_INTERVAL` | `discoveryInterval`    | `5m`           | How often to refresh the list of discovered resources (e.g., `5m`, `1h`). |
| Metrics Cache Duration | `-metrics.cache-duration`     | `CONFLUENT_EXPORTER_METRICS_CACHE_DURATION` | `metricsCacheDuration` | `1m`           | How long to cache the metrics API response (e.g., `1m`, `30s`).           |
| Target Environments    | `-discovery.target-environments` | `CONFLUENT_EXPORTER_TARGET_ENVIRONMENT_IDS` | `targetEnvironmentIDs` | *(empty list)* | Optional comma-separated list of Environment IDs to discover within.        |
| Config File Path       | `-config.file`                | `CONFLUENT_EXPORTER_CONFIG_FILE`     | *(N/A)*                |                | Path to the YAML configuration file.                                        |

### Example `config.yaml`

```yaml
# Confluent Cloud credentials
apiKey: "YOUR_API_KEY"
apiSecret: "YOUR_API_SECRET"

# Server settings
listenAddress: ":9184"
logLevel: "info"

# Discovery and Caching
discoveryInterval: 15m
metricsCacheDuration: 1m

# Optional: Filter discovery to specific environments
# targetEnvironmentIDs:
#   - "env-123abc"
#   - "env-456def"
```

### Running Directly

```bash
# Using flags
./confluent-cloud-exporter \
  -confluent.api-key="YOUR_KEY" \
  -confluent.api-secret="YOUR_SECRET" \
  -log.level=debug

# Using environment variables
export CONFLUENT_EXPORTER_API_KEY="YOUR_KEY"
export CONFLUENT_EXPORTER_API_SECRET="YOUR_SECRET"
./confluent-cloud-exporter

# Using a config file
./confluent-cloud-exporter -config.file=./config.yaml
```

## Deployment

### Docker

A multi-stage `Dockerfile` is provided to build a minimal container image.

1.  **Build:**
    ```bash
    docker build -t your-registry/confluent-cloud-exporter:latest .
    ```
2.  **Run (using environment variables):**
    ```bash
    docker run -d -p 9184:9184 \
      --name confluent-exporter \
      -e CONFLUENT_EXPORTER_API_KEY="YOUR_KEY" \
      -e CONFLUENT_EXPORTER_API_SECRET="YOUR_SECRET" \
      -e CONFLUENT_EXPORTER_LOG_LEVEL="info" \
      your-registry/confluent-cloud-exporter:latest
    ```
3.  **Run (mounting config file):**
    ```bash
    docker run -d -p 9184:9184 \
      --name confluent-exporter \
      -v $(pwd)/config.yaml:/app/config.yaml \
      your-registry/confluent-cloud-exporter:latest -config.file=/app/config.yaml
    ```

### Kubernetes

Example `Deployment` and `Service` manifests are provided in the `K8S/` directory.

1.  **Create Secret:** Store your API credentials securely.
    ```bash
    kubectl create secret generic confluent-cloud-credentials \
      --from-literal=apiKey='YOUR_API_KEY' \
      --from-literal=apiSecret='YOUR_API_SECRET'
    ```
2.  **(Optional) Create ConfigMap:** If using file-based config.
    ```bash
    kubectl create configmap confluent-exporter-config --from-file=config.yaml=./config.yaml
    ```
3.  **Update Deployment:** Modify `K8S/Deployment.yaml` to point to your container image registry.
4.  **Apply Manifests:**
    ```bash
    kubectl apply -f K8S/Deployment.yaml
    kubectl apply -f K8S/Service.yaml
    ```

Prometheus can then discover and scrape the exporter using the `confluent-cloud-exporter` Service.

## Metrics Exposed

This exporter uses the `/v2/metrics/cloud/export` endpoint of the Confluent Cloud Metrics API. It exposes metrics as returned by that endpoint, typically prefixed with `confluent_`.

Key metrics include those for:

-   Kafka (`confluent_kafka_...`)
-   Connect (`confluent_kafka_connect_...`)
-   Schema Registry (`confluent_schema_registry_...`)
-   ksqlDB (`confluent_ksql_...`)
-   Flink / Compute Pools (`confluent_flink_...`)

The exporter automatically enriches these metrics with labels discovered via the Management API, such as:

-   `environment_id`
-   `environment_name`
-   `cluster_id` (where applicable)
-   `name` (human-readable resource name)
-   `display_name`
-   Other resource-specific labels (e.g., `cloud`, `region`, `connector_type`)

Refer to the [Confluent Cloud Metrics API Documentation](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html) for a full list of available metrics.

## Health Check

A simple health check endpoint is available at `/healthz`. It returns `200 OK` if the HTTP server is running.

```bash
curl http://localhost:9184/healthz
```
