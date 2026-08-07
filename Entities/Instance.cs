namespace DotnetConcurrency.Entities;

public class Instance
{
    public Guid Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Status { get; set; } = "Idle";

    public string? LastProcessedBy { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }

    /// <summary>
    /// EF concurrency token — safety net if in-process serialization is bypassed.
    /// </summary>
    public int Version { get; set; }
}
