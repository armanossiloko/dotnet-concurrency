using DotnetConcurrency.Concurrency;
using DotnetConcurrency.Data;
using Microsoft.EntityFrameworkCore;

namespace DotnetConcurrency.Workers;

/// <summary>
/// Shared loop for the three background workers.
/// Each cycle: load instances into a FIFO queue, process available ids, and
/// requeue busy ids behind available work.
/// </summary>
public abstract class InstanceProcessingWorker(
    IServiceScopeFactory scopeFactory,
    InstanceGate gate,
    ILogger logger) : BackgroundService
{
    protected abstract string WorkerName { get; }

    protected abstract TimeSpan Interval { get; }

    protected abstract TimeSpan SimulatedWork { get; }

    protected virtual TimeSpan LockTimeout => TimeSpan.FromMilliseconds(50);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("{Worker} started", WorkerName);

        // Stagger startup so the three workers don't all hit the same second.
        await Task.Delay(Random.Shared.Next(200, 1500), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "{Worker} cycle failed", WorkerName);
            }

            await Task.Delay(Interval, stoppingToken);
        }
    }

    private async Task ProcessOnceAsync(CancellationToken stoppingToken)
    {
        IReadOnlyList<Guid> ids;
        await using (var readScope = scopeFactory.CreateAsyncScope())
        {
            var readDb = readScope.ServiceProvider.GetRequiredService<AppDbContext>();
            ids = await readDb.Instances
                .AsNoTracking()
                .Select(x => x.Id)
                .ToListAsync(stoppingToken);
        }

        var pendingIds = new Queue<Guid>(ids);
        var consecutiveDeferrals = 0;

        while (pendingIds.TryDequeue(out var id) && !stoppingToken.IsCancellationRequested)
        {
            await using var lease = await gate.TryAcquireForWorkerAsync(id, LockTimeout, stoppingToken);
            if (lease is null)
            {
                // Put this id at the back so another instance that is available
                // now can be processed first.
                pendingIds.Enqueue(id);
                consecutiveDeferrals++;

                logger.LogInformation(
                    "{Worker} requeued {InstanceId} (write lock busy — user priority)",
                    WorkerName,
                    id);

                // Every remaining id was deferred without a successful write.
                // Stop rather than looping forever while a controller owns them.
                if (consecutiveDeferrals >= pendingIds.Count)
                {
                    logger.LogInformation(
                        "{Worker} ended this cycle; all {DeferredCount} remaining instances are busy",
                        WorkerName,
                        pendingIds.Count);
                    break;
                }

                continue;
            }

            consecutiveDeferrals = 0;

            await using var writeScope = scopeFactory.CreateAsyncScope();
            var writeDb = writeScope.ServiceProvider.GetRequiredService<AppDbContext>();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken,
                lease.CancellationToken);

            try
            {
                await ProcessInstanceAsync(writeDb, id, linked.Token);
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(
                    "{Worker} yielded {InstanceId} after user requested the write lock",
                    WorkerName,
                    id);
            }
            catch (DbUpdateConcurrencyException ex)
            {
                // The gate prevents this inside one process. This is a safety net for
                // writes made outside this process; retry on the next worker cycle.
                logger.LogWarning(ex, "{Worker} deferred {InstanceId} after an external write conflict", WorkerName, id);
            }
            catch (DbUpdateException ex)
            {
                // Do not let a transient database failure end the hosted service.
                logger.LogWarning(ex, "{Worker} deferred {InstanceId} after a database write failure", WorkerName, id);
            }
        }
    }

    private async Task ProcessInstanceAsync(AppDbContext db, Guid id, CancellationToken ct)
    {
        var entity = await db.Instances.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return;
        }

        logger.LogInformation("{Worker} processing {InstanceId} ({Name})", WorkerName, id, entity.Name);

        // Long enough that a concurrent PUT / hold-lock can cancel us mid-work.
        await Task.Delay(SimulatedWork, ct);

        entity.Status = $"ProcessedBy:{WorkerName}";
        entity.LastProcessedBy = WorkerName;
        entity.UpdatedAt = DateTimeOffset.UtcNow;

        await db.SaveChangesAsync(ct);

        logger.LogInformation("{Worker} saved {InstanceId}", WorkerName, id);
    }
}
