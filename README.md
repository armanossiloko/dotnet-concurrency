## Dotnet Concurrency demo

Minimal ASP.NET Core API that serializes **writes** to `Instance` rows in-process, gives the HTTP controller priority over three background workers, and leaves **GET** endpoints unlocked.

See [HOW_TO_USE.md](HOW_TO_USE.md) for integration and usage examples.

### How it works

| Caller | Behavior |
|---|---|
| `GET` | No `InstanceGate` — never waits on workers |
| `PUT` | Updates immediately when free; otherwise returns `202 Accepted` and queues a priority update |
| `hold-lock` | Waits for active work, then holds the keyed write lock for the demo |
| Background workers | **Try-acquire** (~50ms); if busy, skip and try the next instance |

`InstanceGate` is a singleton keyed by `Instance.Id`. It records pending controller writes, so a worker cannot begin after the controller has claimed the key. A worker that already owns the lease always finishes normally. `InstanceUpdateQueue` applies accepted updates after the active lease is released. An integer `Version` concurrency token is a DB safety net only.

The Npgsql retry strategy retries transient database failures three times. If a write still conflicts with a writer outside this process, the API returns `503 Service Unavailable` rather than exposing an EF/Postgres exception; a worker defers that item until its next cycle.

### Prerequisites

- .NET 10 SDK
- Docker

### Run

```bash
docker compose up -d
dotnet ef database update   # first time / after migration changes
dotnet run
```

On first start, `Program.cs` also runs `MigrateAsync` and seeds three instances (`alpha`, `beta`, `gamma`).

### Try the priority behavior

1. Start the API and watch the console — `CleanupWorker`, `MetricsWorker`, and `SyncWorker` log every ~15–25s.
2. Call `POST /api/instance/{id}/hold-lock?seconds=8` for `11111111-1111-1111-1111-111111111111`.
3. Observe workers log `requeued ... (update busy or priority pending)`.
4. Meanwhile `GET /api/instance/{id}` still returns immediately (reads do not take the write gate).

Sample requests are in `DotnetConcurrency.http`.

### Tests

```bash
dotnet test DotnetConcurrency.slnx -c Release
```

The gate tests cover priority claims, completion of active worker updates, parallel updates to different instances, cancelled waits, and idle-key cleanup.

### Endpoints

- `GET /api/instance` — list (read, unlocked)
- `GET /api/instance/{id}` — detail (read, unlocked)
- `POST /api/instance` — create
- `PUT /api/instance/{id}` — immediate priority update, or `202 Accepted` when queued
- `POST /api/instance/{id}/hold-lock?seconds=5` — demo long-held user lock

### Note on scale-out

This gate is **in-process**. It guarantees one writer per `Instance` only when the controller and workers run in this one application process. Do not run multiple API replicas (or another service that writes `Instances`) with this sample; doing so requires a distributed keyed lock or database-level coordination.

The accepted-update queue is also in memory; use durable messaging if queued operations must survive process restarts.
