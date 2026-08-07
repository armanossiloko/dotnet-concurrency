using DotnetConcurrency.Concurrency;

namespace DotnetConcurrency.Workers;

public sealed class CleanupWorker(
    IServiceScopeFactory scopeFactory,
    InstanceGate gate,
    ILogger<CleanupWorker> logger)
    : InstanceProcessingWorker(scopeFactory, gate, logger)
{
    protected override string WorkerName => "CleanupWorker";

    protected override TimeSpan Interval => TimeSpan.FromSeconds(15);

    protected override TimeSpan SimulatedWork => TimeSpan.FromSeconds(2);
}

public sealed class MetricsWorker(
    IServiceScopeFactory scopeFactory,
    InstanceGate gate,
    ILogger<MetricsWorker> logger)
    : InstanceProcessingWorker(scopeFactory, gate, logger)
{
    protected override string WorkerName => "MetricsWorker";

    protected override TimeSpan Interval => TimeSpan.FromSeconds(20);

    protected override TimeSpan SimulatedWork => TimeSpan.FromSeconds(3);
}

public sealed class SyncWorker(
    IServiceScopeFactory scopeFactory,
    InstanceGate gate,
    ILogger<SyncWorker> logger)
    : InstanceProcessingWorker(scopeFactory, gate, logger)
{
    protected override string WorkerName => "SyncWorker";

    protected override TimeSpan Interval => TimeSpan.FromSeconds(25);

    protected override TimeSpan SimulatedWork => TimeSpan.FromSeconds(4);
}
