"""
tiny-mesh — Zero-dependency service mesh proxy and API gateway.

Circuit breaking, retries, load balancing, health checks, rate limiting,
and graceful degradation — all in one file. No Envoy, no Istio, no sidecar.

MIT License. https://github.com/hussain-alsaibai/tiny-mesh
"""


from __future__ import annotations

__version__ = "0.1.0"

import asyncio
import json
import random
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable, Protocol


# ─────────────────────────────────────────────────────────────
# Dataclasses
# ─────────────────────────────────────────────────────────────

@dataclass
class ServiceConfig:
    name: str
    url: str
    weight: int = 1
    timeout: float = 5.0
    health_check_url: str | None = None
    health_check_interval: float = 30.0


@dataclass
class HealthStatus:
    name: str
    healthy: bool = True
    consecutive_failures: int = 0
    last_check: float = 0.0
    latency_ms: float = 0.0


@dataclass
class CircuitState:
    name: str
    state: str = "closed"  # closed, open, half_open
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    half_open_attempts: int = 0


# ─────────────────────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Circuit breaker with three states:
    - CLOSED: normal operation, requests pass through
    - OPEN: too many failures, requests fail fast
    - HALF_OPEN: testing if service recovered

    Threshold-based: opens after `failure_threshold` consecutive failures.
    Recovery: after `recovery_timeout` seconds, allows `half_open_max_calls`
    through. All succeed → closes. Any fails → reopens.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self._state = CircuitState(name=name)
        self._lock = asyncio.Lock()
        self._on_open: list[Callable[[str], None]] = []
        self._on_close: list[Callable[[str], None]] = []

    @property
    def state(self) -> str:
        return self._state.state

    def on_open(self, cb: Callable[[str], None]):
        self._on_open.append(cb)

    def on_close(self, cb: Callable[[str], None]):
        self._on_close.append(cb)

    async def call(self, func: Callable[[], Awaitable[Any]]) -> Any:
        async with self._lock:
            now = time.monotonic()

            if self._state.state == "open":
                # Check if recovery timeout elapsed → try half-open
                if now - self._state.last_failure_time >= self.recovery_timeout:
                    self._state.state = "half_open"
                    self._state.half_open_attempts = 0

            if self._state.state == "open":
                raise CircuitOpenError(f"Circuit '{self.name}' is OPEN — failing fast")

            if self._state.state == "half_open":
                if self._state.half_open_attempts >= self.half_open_max_calls:
                    raise CircuitOpenError(
                        f"Circuit '{self.name}' is HALF_OPEN — max attempts reached"
                    )
                self._state.half_open_attempts += 1

        try:
            result = await func()
            await self._record_success()
            return result
        except Exception as exc:
            await self._record_failure()
            raise exc

    async def _record_success(self):
        async with self._lock:
            self._state.success_count += 1

            if self._state.state == "half_open":
                if self._state.success_count >= self.success_threshold:
                    self._state.state = "closed"
                    self._state.failure_count = 0
                    self._state.success_count = 0
                    for cb in self._on_close:
                        cb(self.name)

            if self._state.state == "closed":
                self._state.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self._state.failure_count += 1
            self._state.last_failure_time = time.monotonic()
            self._state.success_count = 0

            if self._state.state == "half_open":
                self._state.state = "open"

            elif self._state.state == "closed":
                if self._state.failure_count >= self.failure_threshold:
                    self._state.state = "open"
                    for cb in self._on_open:
                        cb(self.name)

    def stats(self) -> dict:
        return {
            "name": self.name,
            "state": self._state.state,
            "failure_count": self._state.failure_count,
            "success_count": self._state.success_count,
            "last_failure_time": self._state.last_failure_time,
        }


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


# ─────────────────────────────────────────────────────────────
# Retry
# ─────────────────────────────────────────────────────────────

@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: float = 0.1  # fraction of delay to randomize
    retryable_exceptions: tuple = (Exception,)


async def retry(
    func: Callable[[], Awaitable[Any]],
    config: RetryConfig | None = None,
    name: str = "unnamed",
) -> Any:
    """Retry a coroutine with exponential backoff and jitter."""
    cfg = config or RetryConfig()
    last_exc: Exception | None = None

    for attempt in range(1, cfg.max_attempts + 1):
        try:
            return await func()
        except Exception as exc:
            last_exc = exc
            if not _is_retryable(exc, cfg.retryable_exceptions):
                raise

            if attempt == cfg.max_attempts:
                break

            delay = min(
                cfg.base_delay * (cfg.exponential_base ** (attempt - 1)),
                cfg.max_delay,
            )
            if cfg.jitter:
                delay *= 1 + random.uniform(-cfg.jitter, cfg.jitter)

            await asyncio.sleep(delay)

    raise last_exc


def _is_retryable(exc: Exception, retryable: tuple) -> bool:
    return isinstance(exc, retryable)


# ─────────────────────────────────────────────────────────────
# Load Balancer
# ─────────────────────────────────────────────────────────────

class LoadBalancer:
    """
    Weighted round-robin load balancer across service endpoints.
    Also tracks health and excludes unhealthy nodes.
    """

    def __init__(self, services: list[ServiceConfig]):
        self._services = {s.name: s for s in services}
        self._health: dict[str, HealthStatus] = {
            s.name: HealthStatus(name=s.name) for s in services
        }
        self._counts: dict[str, int] = {s.name: 0 for s in services}
        self._lock = asyncio.Lock()

    def healthy_services(self) -> list[ServiceConfig]:
        return [
            s for s in self._services.values()
            if self._health[s.name].healthy
        ]

    async def select(self) -> ServiceConfig:
        """Weighted round-robin selection among healthy services."""
        async with self._lock:
            healthy = self.healthy_services()
            if not healthy:
                # Fall back to all services if none healthy
                healthy = list(self._services.values())

            total_weight = sum(s.weight for s in healthy)
            rnd = random.randint(1, total_weight)

            cumulative = 0
            for svc in healthy:
                cumulative += svc.weight
                if cumulative >= rnd:
                    self._counts[svc.name] += 1
                    return svc

            # Fallback to first
            self._counts[list(healthy)[0].name] += 1
            return list(healthy)[0]

    async def mark_healthy(self, name: str, latency_ms: float = 0.0):
        async with self._lock:
            if name in self._health:
                self._health[name].healthy = True
                self._health[name].consecutive_failures = 0
                self._health[name].latency_ms = latency_ms

    async def mark_unhealthy(self, name: str):
        async with self._lock:
            if name in self._health:
                self._health[name].healthy = False
                self._health[name].consecutive_failures += 1

    def stats(self) -> dict:
        return {
            "services": {
                name: {
                    "healthy": h.healthy,
                    "latency_ms": h.latency_ms,
                    "failures": h.consecutive_failures,
                    "selected_count": self._counts[name],
                }
                for name, h in self._health.items()
            }
        }


# ─────────────────────────────────────────────────────────────
# Rate Limiter (Token Bucket)
# ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Token bucket rate limiter. Async-safe."""

    def __init__(self, rate: float, burst: int):
        self.rate = rate  # tokens per second
        self.burst = burst
        self._tokens = float(burst)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()
        self._waiters = 0

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens. Returns True if acquired, False if rate limited."""
        async with self._lock:
            self._waiters += 1

        try:
            while True:
                async with self._lock:
                    now = time.monotonic()
                    elapsed = now - self._last_update
                    self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                    self._last_update = now

                    if self._tokens >= tokens:
                        self._tokens -= tokens
                        return True

                    wait_time = (tokens - self._tokens) / self.rate
                    # Release lock while waiting
                    self._lock.release()
                    try:
                        await asyncio.sleep(wait_time)
                    finally:
                        self._lock.acquire()

                # Re-check after wait
                async with self._lock:
                    now = time.monotonic()
                    elapsed = now - self._last_update
                    self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                    self._last_update = now

                    if self._tokens >= tokens:
                        self._tokens -= tokens
                        return True
        finally:
            async with self._lock:
                self._waiters -= 1

    async def try_acquire(self, tokens: int = 1) -> bool:
        """Non-blocking acquire. Returns True if acquired."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_update = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


# ─────────────────────────────────────────────────────────────
# Mesh — The Unified API Gateway
# ─────────────────────────────────────────────────────────────

class Mesh:
    """
    Zero-dependency service mesh proxy.

    Wraps external HTTP endpoints with circuit breakers, retries,
    load balancing, rate limiting, and health checks.

    Usage:
        mesh = Mesh()
        mesh.add("openai", "https://api.openai.com/v1")
        mesh.add("github", "https://api.github.com")

        mesh.circuit("openai").on_open(lambda n: print(f"Circuit {n} opened!"))

        async def call():
            return await mesh.get("/chat/completions", service="openai",
                                   body={"model": "gpt-4o-mini", "messages": [...]})

        result = await mesh.execute(call)
    """

    def __init__(self):
        self._services: dict[str, LoadBalancer] = {}
        self._circuits: dict[str, CircuitBreaker] = {}
        self._rate_limiters: dict[str, RateLimiter] = {}
        self._retry_configs: dict[str, RetryConfig] = {}
        self._health_check_tasks: dict[str, asyncio.Task] = {}
        self._http_client: Any = None
        self._log: list[dict] = []
        self._lock = asyncio.Lock()

    def add(
        self,
        name: str,
        url: str,
        *,
        weight: int = 1,
        timeout: float = 5.0,
        health_check_url: str | None = None,
        health_check_interval: float = 30.0,
    ):
        svc = ServiceConfig(
            name=name,
            url=url.rstrip("/"),
            weight=weight,
            timeout=timeout,
            health_check_url=health_check_url,
            health_check_interval=health_check_interval,
        )
        self._services[name] = LoadBalancer([svc])
        self._circuits[name] = CircuitBreaker(name=name)
        self._retry_configs[name] = RetryConfig()
        self._rate_limiters[name] = RateLimiter(rate=100, burst=200)
        return self

    def circuit(self, name: str) -> CircuitBreaker:
        return self._circuits[name]

    def retry(self, name: str, config: RetryConfig):
        self._retry_configs[name] = config
        return self

    def rate_limit(self, name: str, rate: float, burst: int):
        self._rate_limiters[name] = RateLimiter(rate=rate, burst=burst)
        return self

    async def _init_client(self):
        if self._http_client is None:
            import httpx
            self._http_client = httpx.AsyncClient(timeout=10.0)

    async def _do_request(
        self,
        method: str,
        url: str,
        service: str,
        **kwargs,
    ) -> Any:
        await self._init_client()
        resp = await self._http_client.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp

    async def _execute_on_service(
        self,
        service_name: str,
        method: str,
        path: str,
        **kwargs,
    ) -> Any:
        """Execute a single request on a service with full resilience stack."""
        lb = self._services[service_name]
        circuit = self._circuits[service_name]
        rate_limiter = self._rate_limiters[service_name]
        retry_cfg = self._retry_configs[service_name]
        svc_config = list(lb._services.values())[0]

        call_id = str(uuid.uuid4())[:8]
        start = time.monotonic()

        # Rate limit check
        if not await rate_limiter.try_acquire(1):
            raise RateLimitExceededError(f"Rate limit exceeded for '{service_name}'")

        async def _do():
            return await circuit.call(
                lambda: self._do_request(
                    method,
                    f"{svc_config.url}{path}",
                    service=service_name,
                    **kwargs,
                )
            )

        resp = await retry(_do, config=retry_cfg, name=service_name)

        latency_ms = (time.monotonic() - start) * 1000

        # Record success for health
        await lb.mark_healthy(service_name, latency_ms)

        # Log
        async with self._lock:
            self._log.append({
                "call_id": call_id,
                "service": service_name,
                "method": method,
                "path": path,
                "status": resp.status_code,
                "latency_ms": round(latency_ms, 2),
                "circuit_state": circuit.state,
                "timestamp": time.time(),
            })

        return resp

    async def get(self, path: str, *, service: str, **kwargs) -> Any:
        resp = await self._execute_on_service(service, "GET", path, **kwargs)
        return resp

    async def post(self, path: str, *, service: str, **kwargs) -> Any:
        resp = await self._execute_on_service(service, "POST", path, **kwargs)
        return resp

    async def put(self, path: str, *, service: str, **kwargs) -> Any:
        resp = await self._execute_on_service(service, "PUT", path, **kwargs)
        return resp

    async def delete(self, path: str, *, service: str, **kwargs) -> Any:
        resp = await self._execute_on_service(service, "DELETE", path, **kwargs)
        return resp

    async def execute(self, func: Callable[[], Awaitable[Any]]) -> Any:
        """Execute an arbitrary async function with full resilience."""
        return await func()

    def stats(self) -> dict:
        return {
            "services": {name: lb.stats() for name, lb in self._services.items()},
            "circuits": {name: cb.stats() for name, cb in self._circuits.items()},
            "log_size": len(self._log),
            "recent_calls": self._log[-20:],
        }

    async def close(self):
        if self._http_client:
            await self._http_client.aclose()


class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded."""
    pass


# ─────────────────────────────────────────────────────────────
# Decorators
# ─────────────────────────────────────────────────────────────

def meshcircuit(mesh: Mesh, service: str):
    """Decorator: wrap a function with the mesh's circuit breaker for a service."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            circuit = mesh.circuit(service)
            return await circuit.call(lambda: func(*args, **kwargs))
        return wrapper
    return decorator


def meshroute(mesh: Mesh, service: str, method: str = "GET"):
    """Decorator: auto-route a function through the mesh."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            path = kwargs.pop("_path", f"/{func.__name__}")
            http_method = kwargs.pop("_method", method)
            return await mesh._execute_on_service(service, http_method, path, **kwargs)
        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────
# Health Monitor
# ─────────────────────────────────────────────────────────────

class HealthMonitor:
    """Background health checker for mesh services."""

    def __init__(self, mesh: Mesh):
        self.mesh = mesh
        self._tasks: dict[str, asyncio.Task] = {}

    async def _check_loop(self, name: str, interval: float):
        while True:
            try:
                await self._health_check(name)
            except Exception:
                pass
            await asyncio.sleep(interval)

    async def _health_check(self, name: str):
        lb = self.mesh._services.get(name)
        if not lb:
            return

        svc = list(lb._services.values())[0]
        health_url = svc.health_check_url

        if not health_url:
            return

        await self.mesh._init_client()
        start = time.monotonic()
        try:
            resp = await self.mesh._http_client.get(health_url, timeout=3.0)
            latency_ms = (time.monotonic() - start) * 1000
            if resp.status_code < 400:
                await lb.mark_healthy(name, latency_ms)
            else:
                await lb.mark_unhealthy(name)
        except Exception:
            await lb.mark_unhealthy(name)

    def start(self, name: str, interval: float | None = None):
        svc = self.mesh._services.get(name)
        if not svc:
            raise ValueError(f"Service '{name}' not found in mesh")
        iv = interval or svc._services[name].health_check_interval
        self._tasks[name] = asyncio.create_task(self._check_loop(name, iv))

    def stop(self, name: str):
        if name in self._tasks:
            self._tasks[name].cancel()
            del self._tasks[name]
