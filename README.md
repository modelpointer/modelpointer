# ModelPointer AI Gateway

A high-performance AI gateway written in Rust. It sits in front of your LLM backends and exposes a unified API surface to clients, handling routing, load balancing, rate limiting, and observability.

## Features

- **OpenAI and Anthropic protocol support** — `/v1/chat/completions`, `/v1/messages`, `/v1/embeddings`, `/v1/responses`
- **API key management** — issue independent keys to each downstream service; per-key rate limits let you control exactly how much capacity each consumer gets
- **Rate limiting** — per-key-model and per-model sliding-window limits (RPM / TPM); in-process or Redis-backed
- **Weighted routing** — smooth weighted round-robin (SWRR) and consistent-hash strategies per model
- **Primary / fallback tiers** — spill traffic to fallback backends when the primary tier exceeds its capacity threshold, with no 429 returned to the client
- **Per-protocol independence** — openai and anthropic groups for the same model are fully independent, each with its own backends, strategy, and capacity
- **Circuit breaker** — automatically marks unhealthy upstreams and recovers them on success
- **Two config modes** — YAML files with hot-reload, or database (SQLite / PostgreSQL) with periodic polling
- **TLS termination** — optional TLS with PEM certificate and key
- **Observability** — structured JSON access logs, Prometheus metrics, OpenTelemetry tracing

---

## Quick Start

This section walks through getting the gateway running in under 5 minutes, using OpenAI as the upstream backend.

### 1. Install

```bash
git clone https://github.com/modelpointer/modelpointer
cd modelpointer
cargo build --release
# Binary is at: target/release/modelpointer
# Optionally copy it to your PATH:
cp target/release/modelpointer /usr/local/bin/
```

Requires Rust 1.85 or later. See [Docker](#docker) if you prefer a container.

### 2. Configure routes

Create a `routes.yaml` file:

```yaml
upstreams:
  Aliyun:
    api_key: ${DASHSCOPE_API_KEY}
    regions:
      default:
        openai:
          base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
        anthropic:
          base_url: https://dashscope.aliyuncs.com/apps/anthropic/v1
routes:
- model: qwen3.6-plus
  openai:
    upstreams:
    - name: Aliyun.default
      weight: 100
  anthropic:
    upstreams:
    - name: Aliyun.default
      weight: 100
- model: text-embedding-v4
  openai:
    upstreams:
    - name: Aliyun.default
      weight: 100
```

`${DASHSCOPE_API_KEY}` is resolved from the environment at startup. No actual key goes in the file.

### 3. Start the gateway

```bash
export DASHSCOPE_API_KEY=sk-...
modelpointer serve --route-file routes.yaml
```

The gateway listens on `http://localhost:8080`. Authentication is disabled when `--auth-file` is omitted — convenient for local testing.

Expected output:

```
INFO modelpointer: listening on 0.0.0.0:8080
INFO modelpointer: prometheus metrics on 0.0.0.0:29000
```

### 4. Send a request

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6-plus",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

The gateway proxies the request to Aliyun and streams the response back. From the client's perspective it behaves exactly like the OpenAI API.

### 5. Issue API keys to your services

One of the core use cases for ModelPointer is acting as a controlled access layer between your internal services and LLM backends — each downstream service gets its own key, and you control per-key rate limits independently.

Generate a key for each consumer and save it to `auth.yaml`:

```bash
# Issue a key for your backend service
modelpointer key generate --name "backend-service" --append auth.yaml
# Output: tp-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  ← distribute this to the service

# Issue a separate key for your data pipeline
modelpointer key generate --name "data-pipeline" --append auth.yaml
# Output: tp-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy
```

Restart with auth enabled:

```bash
modelpointer serve --route-file routes.yaml --auth-file auth.yaml
```

Each service now calls the gateway with its own key:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer tp-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
  -H "Content-Type: application/json" \
  -d '{"model": "gpt-4o", "messages": [{"role": "user", "content": "Hello!"}]}'
```

> **Authentication is required in production.** Running without `--auth-file` disables all access control — suitable only for local testing.

> **Do not commit `auth.yaml`** — it contains key hashes.

---

## Routing: Primary / Fallback and Per-Protocol Groups

### The typical use case

A common deployment is a self-hosted GPU cluster as the primary backend, with a public cloud provider as the fallback. Clients always send requests to the gateway; the gateway decides where to send them.

```
Client → Gateway → primary: vLLM on your GPU cluster
                 ↘ fallback: OpenAI / Anthropic (when primary is full or down)
```

### Primary tier capacity (`capacity_rpm` / `capacity_tpm`)

The gateway tracks how many requests (RPM) and tokens (TPM) have been sent to the primary tier in the last sliding window. When a threshold is exceeded, the request is transparently forwarded to the fallback tier instead. **No 429 is returned to the client** — from the client's perspective the request succeeds normally.

This is intentionally different from a rate limit. A rate limit protects the gateway itself; primary capacity is a spillover threshold that protects your GPU cluster from overload while keeping clients happy.

```yaml
routes:
  - model: "llama-3"
    openai:
      strategy: swrr
      primary:
        upstreams:
          - name: local-vllm.node-1
            weight: 1
        capacity_rpm: 300      # above 300 req/min → spill to fallback
        capacity_tpm: 500000   # above 500k tokens/min → spill to fallback
      fallback:
        upstreams:
          - name: openai.default
            weight: 1
            upstream_model: "gpt-4o-mini"   # map to a comparable cloud model
```

If no `capacity_rpm` / `capacity_tpm` is set on the primary tier, all traffic stays on the primary until a backend goes unhealthy (circuit breaker), at which point it falls through to the fallback automatically.

### Per-protocol independence

One vLLM instance can serve both the OpenAI-compatible API (`/v1/chat/completions`) and the Anthropic-compatible API (`/v1/messages`) simultaneously. ModelPointer models this correctly: `openai` and `anthropic` protocol groups for the same model are fully independent — each has its own upstreams, strategy, capacity thresholds, and fallback chain.

```yaml
upstreams:
  local-vllm:
    regions:
      node-1:
        openai:
          base_url: "http://10.0.1.10:8000/v1"
        anthropic:
          base_url: "http://10.0.1.10:8000/anthropic/v1"   # same host, different path prefix

  anthropic-cloud:
    api_key: "${ANTHROPIC_API_KEY}"
    regions:
      default:
        anthropic:
          base_url: "https://api.anthropic.com"

  openai-cloud:
    api_key: "${OPENAI_API_KEY}"
    regions:
      default:
        openai:
          base_url: "https://api.openai.com/v1"

routes:
  - model: "my-model"
    # OpenAI protocol: /v1/chat/completions
    openai:
      strategy: swrr
      primary:
        upstreams:
          - name: local-vllm.node-1
            weight: 1
        capacity_rpm: 500
      fallback:
        upstreams:
          - name: openai-cloud.default
            weight: 1
            upstream_model: "gpt-4o-mini"

    # Anthropic protocol: /v1/messages — configured independently
    anthropic:
      strategy: swrr
      primary:
        upstreams:
          - name: local-vllm.node-1
            weight: 1
        capacity_rpm: 300
      fallback:
        upstreams:
          - name: anthropic-cloud.default
            weight: 1
            upstream_model: "claude-haiku-4-5"
```

The two protocol groups share the same physical backend on `node-1` but have separate capacity counters and separate fallback destinations. An openai-protocol request that spills over does not affect the anthropic-protocol capacity counter, and vice versa.

---

## API Endpoints


| Method   | Path                             | Auth     | Description                                      |
| -------- | -------------------------------- | -------- | ------------------------------------------------ |
| `POST`   | `/v1/chat/completions`           | required | OpenAI Chat Completions                          |
| `POST`   | `/v1/messages`                   | required | Anthropic Messages                               |
| `POST`   | `/v1/embeddings`                 | required | OpenAI Embeddings                                |
| `POST`   | `/v1/responses`                  | required | OpenAI Responses (see note below)                |
| `GET`    | `/v1/responses/{id}`             | required | Retrieve a Response object (see note below)      |
| `DELETE` | `/v1/responses/{id}`             | required | Delete a Response object (see note below)        |
| `GET`    | `/v1/responses/{id}/input_items` | required | List input items for a Response (see note below) |
| `GET`    | `/v1/models`                     | none     | List available models                            |
| `GET`    | `/health`                        | none     | Health check                                     |


Authentication is via `Authorization: Bearer <key>`. Omit `--auth-file` / skip database key setup to disable auth entirely.

### Responses API — required headers

The `/v1/responses` family of endpoints requires an explicit `x-tp-provider` header that tells the gateway which upstream to target. Unlike chat completions, a Response object is stateful and lives on a specific provider's server, so the gateway cannot select an upstream automatically on retrieve / delete / input_items.


| Header             | Required | Description                                                                                                         |
| ------------------ | -------- | ------------------------------------------------------------------------------------------------------------------- |
| `x-tp-provider`    | **yes**  | Provider ID to route to, e.g. `openai.default` or `local-vllm.node-1`. Must match the `name` used in `routes.yaml`. |
| `x-tp-routing-key` | no       | Sticky routing key for `weighted_hash` strategy. Falls back to the API key if omitted.                              |


```bash
# Create a response — gateway routes to the specified provider
curl http://localhost:8080/v1/responses \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H "x-tp-provider: openai.default" \
  -H "Content-Type: application/json" \
  -d '{"model": "gpt-4o", "input": "Hello"}'

# Retrieve — must specify the same provider that created the object
curl http://localhost:8080/v1/responses/resp_abc123 \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H "x-tp-provider: openai.default"
```

If `x-tp-provider` is missing or does not match any configured upstream, the gateway returns `400 Bad Request`.

---

## Reference

### Routes Configuration (`routes.yaml`)

```yaml
upstreams:
  <provider-name>:
    api_key: "${ENV_VAR}"       # optional; resolved from environment at startup
    regions:
      <region-name>:
        openai:                 # openai | anthropic
          base_url: "https://..."

routes:
  - model: "model-name"         # name clients use in their requests
    aliases:
      - "alt-name"              # optional alternative names for the same model
    openai:
      strategy: swrr            # swrr | weighted_hash
      upstreams:                # flat list (all primary tier, no fallback)
        - name: provider.region
          weight: 1
          upstream_model: "..."  # optional: model name sent to upstream
          disabled: true         # optional: temporarily remove from rotation
      # — or — primary/fallback split:
      primary:
        upstreams:
          - name: provider.region
            weight: 1
        capacity_rpm: 300        # optional spillover threshold
        capacity_tpm: 500000
      fallback:
        upstreams:
          - name: other-provider.region
            weight: 1
            upstream_model: "fallback-model-name"
    anthropic:
      # same structure as openai, configured independently
```

See `[examples/routes.example.yaml](examples/routes.example.yaml)` for a fully annotated example.

### Hot-reload

In file mode, `routes.yaml`, `auth.yaml`, and `quota.yaml` are all monitored for changes and reloaded automatically — no restart required. The gateway polls each file's modification time every `--upstream-sync-interval-secs` seconds (default: 30). When a change is detected, the new config is applied atomically without dropping in-flight requests.


| File          | What reloads                                                    |
| ------------- | --------------------------------------------------------------- |
| `routes.yaml` | Upstream backends and routing rules                             |
| `auth.yaml`   | Active API keys (new keys take effect within one poll interval) |
| `quota.yaml`  | Per-(key, model) rate-limit overrides                           |


On Kubernetes, mount `routes.yaml` and `quota.yaml` as ConfigMaps and `auth.yaml` as a Secret — the gateway picks up changes automatically when Kubernetes updates the volume. See [Kubernetes Deployment](#kubernetes-deployment) for a complete example.

### Auth Configuration (`auth.yaml`)

```yaml
mode: api_key   # none | api_key

keys:
  - id: "7f3a9c2b-41d4-4a71-b446-655440000000"
    name: "Service A"
    hash: "ba7816bf..."    # SHA-256 of the raw key
  - id: "3d8f1a2b-5678-4c90-d123-456789012345"
    name: "Old Key"
    hash: "e3b0c442..."
    disabled: true
```

### Quota Overrides (`quota.yaml`)

By default, rate limits are set at the model level in `routes.yaml` and apply equally to all API keys. When you need finer control — for example, giving a high-priority service a larger quota while keeping a lower-priority pipeline constrained — use a quota override file.

```yaml
api_key_quotas:
  # High-priority service: higher limits than the model default
  - api_key_id: "7f3a9c2b-41d4-4a71-b446-655440000000"
    model_id: "gpt-4o"
    key_rpm: 1000
    key_tpm: 2000000

  # Batch pipeline: tighter limits to prevent it from crowding out interactive traffic
  - api_key_id: "3d8f1a2b-5678-4c90-d123-456789012345"
    model_id: "gpt-4o"
    key_rpm: 50
    key_tpm: 100000

  # Only key_rpm overridden; key_tpm falls back to the model-level default
  - api_key_id: "3d8f1a2b-5678-4c90-d123-456789012345"
    model_id: "claude-sonnet"
    key_rpm: 20
```

Start the gateway with:

```bash
modelpointer serve \
  --route-file routes.yaml \
  --auth-file auth.yaml \
  --quota-file quota.yaml \
  --rl-window-secs 60
```

`--rl-window-secs` enables rate limiting. Without it, quota overrides are loaded but never enforced.

Override priority (highest to lowest):

1. `quota.yaml` per-(key, model) override
2. Model-level `key_rpm` / `key_tpm` in `routes.yaml`
3. No limit

### Key Management

```bash
# Generate a new key and print it
modelpointer key generate --name "Service A"

# Generate and append directly to auth.yaml
modelpointer key generate --name "Service A" --append auth.yaml

# Disable a key (preserved in the file for audit)
modelpointer key disable <key-id> auth.yaml

# Re-enable a key
modelpointer key enable <key-id> auth.yaml

# List all keys
modelpointer key list auth.yaml
```

### `serve` Options


| Flag                            | Default                              | Description                                      |
| ------------------------------- | ------------------------------------ | ------------------------------------------------ |
| `--host`                        | `0.0.0.0`                            | Listen address                                   |
| `--port`                        | `8080`                               | Listen port                                      |
| `--route-file`                  | —                                    | Path to `routes.yaml`; enables file mode         |
| `--auth-file`                   | —                                    | Path to `auth.yaml`; requires `--route-file`     |
| `--quota-file`                  | —                                    | Path to per-(key, model) rate-limit overrides    |
| `--database-url`                | `sqlite://model_gateway.db?mode=rwc` | Database URL (database mode)                     |
| `--log-dir`                     | —                                    | Directory for JSON access logs                   |
| `--log-level`                   | `info`                               | `trace` / `debug` / `info` / `warn` / `error`    |
| `--json-log`                    | `false`                              | Emit structured JSON logs to stdout              |
| `--tls-cert-path`               | —                                    | PEM certificate file for TLS                     |
| `--tls-key-path`                | —                                    | PEM private key file for TLS                     |
| `--rl-window-secs`              | `60`                                 | Rate-limit sliding window; enables rate limiting |
| `--rl-redis-url`                | —                                    | Redis URL for distributed rate limiting          |
| `--enable-trace`                | `false`                              | Enable OpenTelemetry tracing                     |
| `--otlp-traces-endpoint`        | `http://localhost:4318/v1/traces`    | OTLP endpoint                                    |
| `--prometheus-port`             | `29000`                              | Prometheus metrics port                          |
| `--request-timeout-secs`        | `30`                                 | Per-request upstream timeout                     |
| `--upstream-sync-interval-secs` | `30`                                 | Database polling interval (database mode)        |


### Kubernetes Deployment

In file mode, the three config files map naturally to Kubernetes primitives:


| File          | Kubernetes resource | Reason                                          |
| ------------- | ------------------- | ----------------------------------------------- |
| `routes.yaml` | ConfigMap           | Non-sensitive; safe to store in version control |
| `auth.yaml`   | Secret              | Contains key hashes; treat as sensitive         |
| `quota.yaml`  | ConfigMap           | Non-sensitive                                   |


Kubernetes updates ConfigMap and Secret volume mounts atomically via a symlink swap. ModelPointer detects the mtime change on the next poll and reloads automatically — no rolling restart needed when you add a key or update a quota.

```yaml
# configmap-routes.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: modelpointer-routes
data:
  routes.yaml: |
    upstreams:
      openai:
        api_key: "${OPENAI_API_KEY}"
        regions:
          default:
            openai:
              base_url: "https://api.openai.com/v1"
    routes:
      - model: "gpt-4o"
        openai:
          strategy: swrr
          upstreams:
            - name: openai.default
              weight: 1
---
# secret-auth.yaml  (generate with: modelpointer key generate ...)
apiVersion: v1
kind: Secret
metadata:
  name: modelpointer-auth
stringData:
  auth.yaml: |
    mode: api_key
    keys:
      - id: "7f3a9c2b-41d4-4a71-b446-655440000000"
        name: "backend-service"
        hash: "ba7816bf..."
---
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: modelpointer
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: modelpointer
          image: modelpointer:latest
          args:
            - serve
            - --route-file=/config/routes.yaml
            - --auth-file=/secrets/auth.yaml
            - --rl-window-secs=60
          env:
            - name: OPENAI_API_KEY
              valueFrom:
                secretKeyRef:
                  name: openai-credentials
                  key: api-key
          ports:
            - containerPort: 8080
            - containerPort: 29000   # Prometheus
          volumeMounts:
            - name: routes
              mountPath: /config
            - name: auth
              mountPath: /secrets
      volumes:
        - name: routes
          configMap:
            name: modelpointer-routes
        - name: auth
          secret:
            secretName: modelpointer-auth
```

> **Multi-replica note**: in-process rate limiting (`--rl-window-secs` without `--rl-redis-url`) is per-instance. For accurate cross-pod enforcement, use Redis: `--rl-redis-url redis://redis-svc:6379`.

---

## Observability

- **Access logs**: written to `<log-dir>/modelpointer.json` in JSON format; each record includes the API key ID, model, and input/output token counts — feed these into your analytics pipeline to track per-key token consumption, perform cost allocation, and chargeback across teams
- **Prometheus**: `http://localhost:29000` (configurable via `--prometheus-port`); key metrics:

  | Metric                         | Type      | Description                                                                                                  |
  | ------------------------------ | --------- | ------------------------------------------------------------------------------------------------------------ |
  | `mg_upstream_ttft_seconds`     | histogram | Time to first token per model and provider — directly reflects backend responsiveness for streaming requests |
  | `mg_upstream_tpot_ms`          | histogram | Time per output token (ms/token) per model and provider — reflects sustained generation throughput           |
  | `mg_gateway_duration_seconds`  | histogram | End-to-end request duration including retries                                                                |
  | `mg_upstream_duration_seconds` | histogram | Single-attempt upstream call duration                                                                        |
  | `mg_gateway_requests_total`    | counter   | Request volume by model, endpoint, streaming                                                                 |
  | `mg_gateway_errors_total`      | counter   | Errors by model, endpoint, error type                                                                        |
  | `mg_upstream_requests_total`   | counter   | Per-attempt upstream calls by model, provider, status code                                                   |
  | `mg_retry_attempts_total`      | counter   | Retry count by model and trigger status code                                                                 |
  | `mg_retry_exhausted_total`     | counter   | Requests that exhausted all retries                                                                          |
  | `mg_worker_cb_state`           | gauge     | Circuit breaker state per backend (0=closed, 1=open, 2=half-open)                                            |
  | `mg_worker_available_total`    | gauge     | Available (circuit-closed) backends per model                                                                |

  All metrics are labeled with `model` and `provider`, making it straightforward to compare TTFT and TPOT across different backend deployments and identify performance regressions on a per-model basis.
- **OpenTelemetry**: pass `--enable-trace --otlp-traces-endpoint http://...` to emit spans

---

## Docker

```bash
# Build from the gateway/ root
docker build -f docker/Dockerfile -t modelpointer:latest .

# File mode — mount your config files at runtime
docker run -d \
  -p 8080:8080 \
  -p 29000:29000 \
  -v "$PWD/routes.yaml:/routes.yaml:ro" \
  -v "$PWD/auth.yaml:/auth.yaml:ro" \
  -e DASHSCOPE_API_KEY="$DASHSCOPE_API_KEY" \
  modelpointer:latest serve \
    --route-file /routes.yaml \
    --auth-file /auth.yaml

# Database mode
docker run -d \
  -p 8080:8080 \
  -e DATABASE_URL="postgres://user:pass@db-host/modelpointer" \
  modelpointer:latest serve
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).