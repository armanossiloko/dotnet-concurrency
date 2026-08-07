namespace DotnetConcurrency.Models;

public record InstanceDto(
    Guid Id,
    string Name,
    string Status,
    string? LastProcessedBy,
    DateTimeOffset UpdatedAt);

public record CreateInstanceRequest(string Name);

public record UpdateInstanceRequest(string Name, string Status);
