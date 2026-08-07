# How to use the concurrency solution

This solution coordinates updates to an `Instance` by its `Id`. Reads never take a lock. Updates must hold an `InstanceGate.UpdateLease` for the complete read-modify-save operation.

## Register the services

`Program.cs` registers one shared gate and the accepted-update queue:

```csharp
builder.Services.AddSingleton<InstanceGate>();
builder.Services.AddSingleton<InstanceUpdateQueue>();
builder.Services.AddHostedService(
    services => services.GetRequiredService<InstanceUpdateQueue>());
```

The gate must be a singleton so controllers and all workers use the same keyed state.

## Read an Instance

Reads do not use `InstanceGate`:

```csharp
var instance = await db.Instances
    .AsNoTracking()
    .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
```

A read returns the latest committed database value and does not wait for an update in progress.

## Update from a controller

Start a priority acquisition before accessing the tracked entity:

```csharp
var pendingLease = gate.AcquireForUpdateAsync(
    id,
    applicationLifetime.ApplicationStopping);

if (!pendingLease.IsCompleted)
{
    updateQueue.Enqueue(id, name, status, pendingLease);
    return AcceptedAtAction(nameof(GetById), new { id });
}

await using var lease = await pendingLease;

var instance = await db.Instances.FirstOrDefaultAsync(
    x => x.Id == id,
    cancellationToken);

instance.Name = name;
instance.Status = status;
await db.SaveChangesAsync(cancellationToken);
```

When the instance is free, the request updates it immediately. When a worker already owns it, the request returns `202 Accepted`; `InstanceUpdateQueue` performs the update after the worker finishes.

The controller never cancels an active worker. Its pending priority claim only prevents additional workers from acquiring that instance first.

## Update from a worker

Workers briefly try to acquire the update lease:

```csharp
await using var lease = await gate.TryAcquireForUpdateAsync(
    id,
    TimeSpan.FromMilliseconds(50),
    stoppingToken);

if (lease is null)
{
    pendingIds.Enqueue(id);
    continue;
}

var instance = await db.Instances.FirstOrDefaultAsync(
    x => x.Id == id,
    stoppingToken);

// Process and modify the instance.
await db.SaveChangesAsync(stoppingToken);
```

If the instance is busy or a controller update is pending, the worker receives `null`, moves the id to the back of its queue, and continues with another instance.

## Rules to follow

1. Use the gate only for updates, not GET/read operations.
2. Use the same `Instance.Id` as the gate key everywhere.
3. Acquire the lease before loading the tracked entity.
4. Keep the lease until `SaveChangesAsync` completes.
5. Use a fresh scoped `DbContext` for each background update.
6. Never share a `DbContext` between workers or threads.
7. Always dispose the lease with `await using`.

## Deployment limitation

`InstanceGate` and `InstanceUpdateQueue` are in-memory and coordinate only one application process. Multiple API replicas or other database writers require distributed or database-level coordination.

Queued HTTP updates are also lost if the process stops. Use durable messaging when accepted operations must survive restarts.

