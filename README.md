# tiny-mesh — Zero-Dependency Service Mesh Proxy

> **Circuit breakers, retries, load balancing, rate limiting, and health checks — all in one file.**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](tiny_mesh.py)
[![Part of the tiny-* ecosystem](https://img.shields.io/badge/tiny--*-ecosystem-purple.svg)](#ecosystem)

**Requires httpx for HTTP calls** (`pip install httpx`). If you want truly zero deps, the HTTP primitives are swappable.

## Why tiny-mesh?

Deploying Envoy or Istio for a service mesh is overkill for most applications. tiny-mesh gives you the critical resilience patterns — circuit breakers, retries, rate limiting — in a single Python file you can drop into any project.

- No sidecar proxy
- No YAML configuration files
- No separate mesh infrastructure
- Just import and go

## Features

- **🔴 Circuit Breaker** — Half-open, threshold-based. Opens after N failures, tests recovery periodically.
- **🔁 Retry** — Exponential backoff with jitter. Configurable exceptions.
- **⚖️ Load Balancing** — Weighted round-robin across service endpoints.
- **🚦 Rate Limiter** — Token bucket with burst support.
- **💓 Health Checks** — Background health monitoring with configurable intervals.
- **📊 Observability** — Stats endpoint with call log, circuit states, latency.

## Installation

```bash
pip install httpx  # only external dependency
# Copy tiny_mesh.py into your project — that's it.
```

Or via PyPI:
```bash
pip install tiny-mesh
```

## Quick Start

```python
import asyncio
from tiny_mesh import Mesh, RetryConfig, RateLimitExceededError, CircuitOpenError

mesh = Mesh()

# Add services
mesh.add("openai", "https://api.openai.com/v1", timeout=10.0)
mesh.add("github", "https://api.github.com")
mesh.add("weather", "https://api.weather.example.com")

# Configure circuit breaker callbacks
mesh.circuit("openai").on_open(
    lambda n: print(f"⚠️  Circuit {n} OPENED — failing fast")
)
mesh.circuit("openai").on_close(
    lambda n: print(f"✅  Circuit {n} CLOSED — service recovered")
)

# Configure retries per service
mesh.retry("openai", RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    exponential_base=2.0,
))

# Rate limiting (100 req/s, burst of 200)
mesh.rate_limit("openai", rate=100, burst=200)

async def call_llm():
    try:
        resp = await mesh.post("/chat/completions", service="openai", json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "Hello!"}]
        })
        return resp.json()
    except CircuitOpenError:
        return {"error": "Service unavailable — circuit open"}
    except RateLimitExceededError:
        return {"error": "Rate limited"}

async def main():
    result = await mesh.execute(call_llm)
    print(result)

    # Stats
    print(mesh.stats())

asyncio.run(main())
```

## Circuit Breaker Deep Dive

```python
from tiny_mesh import CircuitBreaker, CircuitOpenError

cb = CircuitBreaker(
    name="my-service",
    failure_threshold=5,       # open after 5 consecutive failures
    success_threshold=2,       # close after 2 successes in half-open
    recovery_timeout=30.0,      # try half-open after 30 seconds
    half_open_max_calls=3,     # allow 3 test calls in half-open
)

# Direct usage
async def risky_call():
    return await external_api()

try:
    result = await cb.call(risky_call)
except CircuitOpenError:
    result = fallback_response()
```

Circuit state machine:
```
CLOSED ──(5 failures)──▶ OPEN ──(30s timeout)──▶ HALF_OPEN
   ▲                         │                        │
   │                         │(any failure)           │(2 successes)
   │◀────────(success)───────┘                        │
   └──────────────────────────────────────────────────┘
```

## Retry Deep Dive

```python
from tiny_mesh import retry, RetryConfig

cfg = RetryConfig(
    max_attempts=4,
    base_delay=0.5,
    exponential_base=2.0,
    jitter=0.2,           # ±20% randomization
    retryable_exceptions=(ConnectionError, TimeoutError, httpx.HTTPStatusError),
)

async def fetch_data():
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://api.example.com/data")
        resp.raise_for_status()
        return resp.json()

result = await retry(fetch_data, config=cfg, name="data-fetch")
```

## Load Balancing

```python
mesh = Mesh()

# Add multiple endpoints for the same service
mesh.add("payments", "https://pay-us.example.com", weight=3)
mesh.add("payments", "https://pay-eu.example.com", weight=2)
mesh.add("payments", "https://pay-asia.example.com", weight=1)

# Weighted round-robin: US gets 3x traffic, EU 2x, Asia 1x
async def process_payment():
    resp = await mesh.get("/charge", service="payments", json={...})
    return resp.json()
```

## Health Checks

```python
from tiny_mesh import HealthMonitor

mesh = Mesh()
mesh.add("database", "https://db.internal",
         health_check_url="https://db.internal/health",
         health_check_interval=15.0)

monitor = HealthMonitor(mesh)
monitor.start("database")

# Run for a while, then stop
await asyncio.sleep(300)
monitor.stop("database")
```

## Decorators

```python
# Wrap any async function with circuit breaking
@meshcircuit(mesh, "openai")
async def call_llm(messages):
    return await httpx.AsyncClient().post(
        "https://api.openai.com/v1/chat/completions",
        json={"model": "gpt-4o", "messages": messages}
    )
```

## Stats & Observability

```python
stats = mesh.stats()

# Circuits
for name, cb in stats["circuits"].items():
    print(f"Circuit {name}: {cb['state']} "
          f"(failures={cb['failure_count']})")

# Recent calls
for call in stats["recent_calls"]:
    print(f"{call['call_id']} {call['method']} {call['path']} "
          f"→ {call['status']} in {call['latency_ms']}ms")

# Service health
for name, svc in stats["services"].items():
    print(f"{name}: healthy={svc['selected_count']} calls")
```

## Comparison

| Feature | tiny-mesh | Envoy | Istio |
|---------|-----------|-------|-------|
| Lines of config | 0 | ~100 YAML | ~200 YAML |
| Sidecar required | No | Yes | Yes |
| Circuit breaker | ✅ | ✅ | ✅ |
| Retry w/ backoff | ✅ | ✅ | ✅ |
| Weighted LB | ✅ | ✅ | ✅ |
| Rate limiting | ✅ | ✅ | ✅ |
| Health checks | ✅ | ✅ | ✅ |
| Setup time | 5 min | Hours | Days |

## Ecosystem

Part of the **tiny-\*** ecosystem — zero-dependency Python utilities:

| Repo | Description |
|------|-------------|
| [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) | Agent framework in one file |
| [tiny-trace](https://github.com/hussain-alsaibai/tiny-trace) | Distributed tracing |
| [tiny-checkpoint](https://github.com/hussain-alsaibai/tiny-checkpoint) | Agent suspend/resume |
| [tiny-repl](https://github.com/hussain-alsaibai/tiny-repl) | Agent debugging REPL |
| [fast-cache](https://github.com/hussain-alsaibai/fast-cache) | LRU + TTL cache |
| [tiny-log](https://github.com/hussain-alsaibai/tiny-log) | Structured logging |
| [tiny-llm-router](https://github.com/hussain-alsaibai/tiny-llm-router) | Multi-provider LLM routing |
| [tiny-llm](https://github.com/hussain-alsaibai/tiny-llm) | Unified LLM client |
| [dev-masterkit](https://github.com/hussain-alsaibai/dev-masterkit) | Curated dev tools |

---

*MIT License. https://github.com/hussain-alsaibai/tiny-mesh*
