using DotnetConcurrency.Concurrency;
using DotnetConcurrency.Data;
using DotnetConcurrency.Infrastructure;
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
            await using var lease = await gate.TryAcquireForUpdateAsync(
                id,
                LockTimeout,
                stoppingToken);
            if (lease is null)
            {
                // Put this id at the back so another instance that is available
                // now can be processed first.
                pendingIds.Enqueue(id);
                consecutiveDeferrals++;

                logger.LogInformation(
                    "{Worker} requeued {InstanceId} (update busy or priority pending)",
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

            try
            {
                await ProcessInstanceAsync(writeDb, id, stoppingToken);
            }
            catch (Exception ex) when (TransientDatabaseErrors.IsRetryable(ex))
            {
                // The gate prevents this inside one process. This is a safety net for
                // transient failures or writes made outside it. Retry next cycle.
                logger.LogWarning(ex, "{Worker} deferred {InstanceId} after a transient database failure", WorkerName, id);
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

        // Long enough to demonstrate that a priority update waits for this
        // already-running operation rather than cancelling it.
        await Task.Delay(SimulatedWork, ct);

        entity.Status = $"ProcessedBy:{WorkerName}";
        entity.LastProcessedBy = WorkerName;

        await db.SaveChangesAsync(ct);

        logger.LogInformation("{Worker} saved {InstanceId}", WorkerName, id);
    }
}
