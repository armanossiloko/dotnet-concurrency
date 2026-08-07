using System.Threading.Channels;
using DotnetConcurrency.Concurrency;
using DotnetConcurrency.Data;
using DotnetConcurrency.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace DotnetConcurrency.Workers;

/// <summary>
/// Completes priority updates which could not acquire an Instance immediately
/// during an HTTP request.
/// </summary>
public sealed class InstanceUpdateQueue(
    IServiceScopeFactory scopeFactory,
    ILogger<InstanceUpdateQueue> logger) : BackgroundService
{
    private readonly Channel<QueuedUpdate> _updates =
        Channel.CreateUnbounded<QueuedUpdate>(new UnboundedChannelOptions
        {
            SingleReader = true,
            AllowSynchronousContinuations = false
        });

    public void Enqueue(
        Guid instanceId,
        string name,
        string status,
        Task<InstanceGate.UpdateLease> pendingLease)
    {
        if (!_updates.Writer.TryWrite(new QueuedUpdate(instanceId, name, status, pendingLease)))
        {
            throw new InvalidOperationException("The instance update queue is unavailable.");
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Parallel.ForEachAsync(
            _updates.Reader.ReadAllAsync(stoppingToken),
            new ParallelOptions
            {
                CancellationToken = stoppingToken,
                MaxDegreeOfParallelism = 8
            },
            ProcessQueuedUpdateAsync);
    }

    private async ValueTask ProcessQueuedUpdateAsync(
        QueuedUpdate update,
        CancellationToken stoppingToken)
    {
        try
        {
            await ApplyAsync(update, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal application shutdown.
        }
        catch (Exception ex) when (TransientDatabaseErrors.IsRetryable(ex))
        {
            logger.LogWarning(
                ex,
                "Queued update for {InstanceId} failed after provider retries",
                update.InstanceId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Queued update for {InstanceId} failed", update.InstanceId);
        }
    }

    private async Task ApplyAsync(QueuedUpdate update, CancellationToken stoppingToken)
    {
        await using var lease = await update.PendingLease.WaitAsync(stoppingToken);
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var entity = await db.Instances.FirstOrDefaultAsync(
            x => x.Id == update.InstanceId,
            stoppingToken);

        if (entity is null)
        {
            logger.LogWarning(
                "Queued update ignored because Instance {InstanceId} no longer exists",
                update.InstanceId);
            return;
        }

        entity.Name = update.Name;
        entity.Status = update.Status;
        entity.LastProcessedBy = "InstanceController/Queued";

        await db.SaveChangesAsync(stoppingToken);
        logger.LogInformation("Applied queued priority update for {InstanceId}", update.InstanceId);
    }

    private sealed record QueuedUpdate(
        Guid InstanceId,
        string Name,
        string Status,
        Task<InstanceGate.UpdateLease> PendingLease);
}
