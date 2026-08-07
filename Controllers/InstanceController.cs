using DotnetConcurrency.Concurrency;
using DotnetConcurrency.Data;
using DotnetConcurrency.Entities;
using DotnetConcurrency.Models;
using DotnetConcurrency.Workers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace DotnetConcurrency.Controllers;

[ApiController]
[Route("api/[controller]")]
public class InstanceController(
    AppDbContext db,
    InstanceGate gate,
    InstanceUpdateQueue updateQueue,
    IHostApplicationLifetime applicationLifetime,
    ILogger<InstanceController> logger) : ControllerBase
{
    /// <summary>
    /// Reads skip the write gate — users never wait on workers for GET.
    /// </summary>
    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<InstanceDto>>> GetAll(CancellationToken ct)
    {
        var items = await db.Instances
            .AsNoTracking()
            .OrderBy(x => x.Name)
            .Select(x => new InstanceDto(x.Id, x.Name, x.Status, x.LastProcessedBy, x.UpdatedAt))
            .ToListAsync(ct);

        return Ok(items);
    }

    /// <summary>
    /// Read-only: no InstanceGate acquisition.
    /// </summary>
    [HttpGet("{id:guid}")]
    public async Task<ActionResult<InstanceDto>> GetById(Guid id, CancellationToken ct)
    {
        var item = await db.Instances.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id, ct);
        return item is null ? NotFound() : Ok(ToDto(item));
    }

    [HttpPost]
    public async Task<ActionResult<InstanceDto>> Create(
        [FromBody] CreateInstanceRequest request,
        CancellationToken ct)
    {
        var entity = new Instance
        {
            Id = Guid.NewGuid(),
            Name = request.Name.Trim(),
            Status = "Created"
        };

        db.Instances.Add(entity);
        await db.SaveChangesAsync(ct);

        return CreatedAtAction(nameof(GetById), new { id = entity.Id }, ToDto(entity));
    }

    /// <summary>
    /// Priority update path. If the Instance is busy, the operation is queued,
    /// new background work is blocked, and the request returns 202 immediately.
    /// </summary>
    [HttpPut("{id:guid}")]
    public async Task<ActionResult<InstanceDto>> Update(
        Guid id,
        [FromBody] UpdateInstanceRequest request,
        CancellationToken ct)
    {
        var name = request.Name.Trim();
        var status = request.Status.Trim();
        var pendingLease = gate.AcquireForUpdateAsync(
            id,
            applicationLifetime.ApplicationStopping);

        if (!pendingLease.IsCompleted)
        {
            updateQueue.Enqueue(id, name, status, pendingLease);
            return AcceptedAtAction(
                nameof(GetById),
                new { id },
                new { id, state = "Queued" });
        }

        await using var _ = await pendingLease;

        var entity = await db.Instances.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return NotFound();
        }

        logger.LogInformation("User updating instance {InstanceId}", id);

        entity.Name = name;
        entity.Status = status;
        entity.LastProcessedBy = "InstanceController";

        // Simulate a bit of work so you can observe workers skipping this id.
        await Task.Delay(TimeSpan.FromMilliseconds(800), ct);

        await db.SaveChangesAsync(ct);
        return Ok(ToDto(entity));
    }

    /// <summary>
    /// Demonstrates user priority: holds the write lock for several seconds.
    /// Call this while workers run; they should skip this instance and log "busy".
    /// GETs for the same id still succeed without waiting.
    /// </summary>
    [HttpPost("{id:guid}/hold-lock")]
    public async Task<ActionResult<InstanceDto>> HoldLock(
        Guid id,
        [FromQuery] int seconds = 5,
        CancellationToken ct = default)
    {
        seconds = Math.Clamp(seconds, 1, 30);

        await using var _ = await gate.AcquireForUpdateAsync(id, ct);

        var entity = await db.Instances.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return NotFound();
        }

        logger.LogWarning(
            "User holding write lock on {InstanceId} for {Seconds}s (workers will skip)",
            id,
            seconds);

        entity.Status = $"UserHold({seconds}s)";
        entity.LastProcessedBy = "InstanceController/HoldLock";

        await Task.Delay(TimeSpan.FromSeconds(seconds), ct);
        await db.SaveChangesAsync(ct);

        return Ok(ToDto(entity));
    }

    private static InstanceDto ToDto(Instance x) =>
        new(x.Id, x.Name, x.Status, x.LastProcessedBy, x.UpdatedAt);
}
