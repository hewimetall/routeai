# PyMCPGateway

PyMCPGateway is a FastAPI service that accepts JSON-RPC requests for configured
MCP services, starts a Kubernetes Job for the selected service, and returns the
job output. Results are cached in Redis when available.

## Requirements

- Python 3.12
- [`uv`](https://docs.astral.sh/uv/) for dependency management
- Access to a Kubernetes cluster
- Redis reachable from the gateway process
- Kubernetes secrets for each enabled MCP service token

## Setup

Install project dependencies into the local virtual environment:

```bash
uv sync
```

Install development dependencies too:

```bash
uv sync --dev
```

## Environment

The current gateway code expects these runtime values:

| Setting | Default | Description |
| --- | --- | --- |
| `JWT_SECRET` | `your-jwt-secret-key` | Secret used to verify and generate HS256 JWTs. |
| `JWT_ALGORITHM` | `HS256` | JWT signing algorithm. |
| `REDIS_HOST` | `192.168.0.13` | Redis host used for response caching. |
| `REDIS_PORT` | `6379` | Redis port used for response caching. |
| `KUBECONFIG` | `./kubeconfig.yml` | Cluster configuration loaded lazily at startup or first Kubernetes call. |
| `REQUEST_TIMEOUT` | `300` | Maximum seconds to wait for a worker Job. |
| `JOB_POLL_INTERVAL` | `2` | Seconds between worker Job status checks. |

Each service in `MCP_SERVICES` also references a Kubernetes secret whose
`token` key is injected into the worker Job. For example, the `github` service
expects a secret named `github-token`.

## Run

Start the API locally:

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

Generate an example JWT:

```bash
uv run python gen.py
```

Call a service endpoint with a bearer token:

```bash
curl -X POST http://localhost:8000/github/ \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":"1"}'
```

## Test and quality checks

Run linting:

```bash
uv run ruff check .
```

Run tests with coverage:

```bash
uv run pytest --cov=. --cov-report=term-missing
```

Audit Python dependencies:

```bash
uv run --with pip-audit pip-audit --progress-spinner off
```

## Notes from the initial audit

- No CI workflow is currently present.
- No automated tests were present before this documentation update.
- The project is Python-only (`pyproject.toml`, `uv.lock`, `.python-version`).
- `pyproject.toml` declared `README.md`, but the file was missing.
