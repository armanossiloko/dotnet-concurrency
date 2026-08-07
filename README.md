## Dotnet Concurrency demo

Minimal ASP.NET Core API that serializes **writes** to `Instance` rows in-process, gives the HTTP controller priority over three background workers, and leaves **GET** endpoints unlocked.

### How it works

| Caller | Behavior |
|---|---|
| `GET` | No `InstanceGate` — never waits on workers |
| `PUT` / `hold-lock` | Cancels workers on that id, then **awaits** the keyed write lock |
| Background workers | **Try-acquire** (~50ms); if busy, skip and try the next instance |

`InstanceGate` is a singleton keyed by `Instance.Id`. It records pending user writes, so a worker cannot begin after a controller has claimed the key. An integer `Version` concurrency token is a DB safety net only.

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
3. Observe workers log `skipped ... (write lock busy — user priority)` or `yielded ... after user requested the write lock`.
4. Meanwhile `GET /api/instance/{id}` still returns immediately (reads do not take the write gate).

Sample requests are in `DotnetConcurrency.http`.

### Endpoints

- `GET /api/instance` — list (read, unlocked)
- `GET /api/instance/{id}` — detail (read, unlocked)
- `POST /api/instance` — create
- `PUT /api/instance/{id}` — user update (priority write)
- `POST /api/instance/{id}/hold-lock?seconds=5` — demo long-held user lock

### Note on scale-out

This gate is **in-process**. It guarantees one writer per `Instance` only when the controller and workers run in this one application process. Do not run multiple API replicas (or another service that writes `Instances`) with this sample; doing so requires a distributed keyed lock or database-level coordination.
