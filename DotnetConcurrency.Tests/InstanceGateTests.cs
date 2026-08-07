using DotnetConcurrency.Concurrency;

namespace DotnetConcurrency.Tests;

public sealed class InstanceGateTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task PriorityUpdate_PreventsBackgroundAcquisition()
    {
        var gate = new InstanceGate();
        var id = Guid.NewGuid();

        await using var priorityLease = await gate.AcquireForUpdateAsync(id, CancellationToken.None);

        var backgroundLease = await gate.TryAcquireForUpdateAsync(
            id,
            TimeSpan.Zero,
            CancellationToken.None);

        Assert.Null(backgroundLease);
    }

    [Fact]
    public async Task PriorityUpdate_WaitsForActiveBackgroundUpdate()
    {
        var gate = new InstanceGate();
        var id = Guid.NewGuid();
        var backgroundLease = await gate.TryAcquireForUpdateAsync(
            id,
            TimeSpan.Zero,
            CancellationToken.None);

        Assert.NotNull(backgroundLease);

        var priorityTask = gate.AcquireForUpdateAsync(id, CancellationToken.None);

        Assert.False(priorityTask.IsCompleted);

        await backgroundLease.DisposeAsync();
        await using var priorityLease = await priorityTask.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task PendingPriorityUpdate_PreventsAnotherBackgroundUpdate()
    {
        var gate = new InstanceGate();
        var id = Guid.NewGuid();
        var activeBackgroundLease = await gate.TryAcquireForUpdateAsync(
            id,
            TimeSpan.Zero,
            CancellationToken.None);

        Assert.NotNull(activeBackgroundLease);

        var priorityTask = gate.AcquireForUpdateAsync(id, CancellationToken.None);

        var secondBackgroundLease = await gate.TryAcquireForUpdateAsync(
            id,
            TimeSpan.Zero,
            CancellationToken.None);

        Assert.Null(secondBackgroundLease);

        await activeBackgroundLease.DisposeAsync();
        await using var priorityLease = await priorityTask.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task DifferentInstances_CanBeUpdatedConcurrently()
    {
        var gate = new InstanceGate();

        await using var first = await gate.AcquireForUpdateAsync(
            Guid.NewGuid(),
            CancellationToken.None);
        await using var second = await gate.AcquireForUpdateAsync(
            Guid.NewGuid(),
            CancellationToken.None);

        Assert.NotNull(first);
        Assert.NotNull(second);
    }

    [Fact]
    public async Task ReleasedInstances_AreRemovedFromGate()
    {
        var gate = new InstanceGate();
        var id = Guid.NewGuid();

        var lease = await gate.AcquireForUpdateAsync(id, CancellationToken.None);
        Assert.Equal(1, gate.TrackedInstanceCount);

        await lease.DisposeAsync();

        Assert.Equal(0, gate.TrackedInstanceCount);
    }

    [Fact]
    public async Task CancelledBackgroundWait_DoesNotLeakGateState()
    {
        var gate = new InstanceGate();
        var id = Guid.NewGuid();
        var activeLease = await gate.TryAcquireForUpdateAsync(
            id,
            TimeSpan.Zero,
            CancellationToken.None);
        Assert.NotNull(activeLease);

        using var cancellation = new CancellationTokenSource();
        var waitingTask = gate.TryAcquireForUpdateAsync(
            id,
            TestTimeout,
            cancellation.Token);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitingTask);
        await activeLease.DisposeAsync();

        Assert.Equal(0, gate.TrackedInstanceCount);
    }
}
