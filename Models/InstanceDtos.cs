using System.ComponentModel.DataAnnotations;

namespace DotnetConcurrency.Models;

public record InstanceDto(
    Guid Id,
    string Name,
    string Status,
    string? LastProcessedBy,
    DateTimeOffset UpdatedAt);

public record CreateInstanceRequest(
    [Required, StringLength(200, MinimumLength = 1)] string Name);

public record UpdateInstanceRequest(
    [Required, StringLength(200, MinimumLength = 1)] string Name,
    [Required, StringLength(100, MinimumLength = 1)] string Status);
