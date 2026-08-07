using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace DotnetConcurrency.Infrastructure;

/// <summary>
/// Prevents database concurrency/provider errors caused by a writer outside this
/// process from surfacing as unhandled API exceptions. The client can retry the
/// request; normal in-process writes are serialized by <see cref="Concurrency.InstanceGate"/>.
/// </summary>
public sealed class DatabaseWriteExceptionFilter(
    ILogger<DatabaseWriteExceptionFilter> logger) : IAsyncExceptionFilter
{
    public Task OnExceptionAsync(ExceptionContext context)
    {
        if (!IsDatabaseWriteFailure(context.Exception))
        {
            return Task.CompletedTask;
        }

        logger.LogWarning(
            context.Exception,
            "Database write for {Path} failed after the provider retry policy",
            context.HttpContext.Request.Path);

        context.Result = new ObjectResult(new ProblemDetails
        {
            Status = StatusCodes.Status503ServiceUnavailable,
            Title = "The instance is temporarily busy",
            Detail = "Please retry the request."
        })
        {
            StatusCode = StatusCodes.Status503ServiceUnavailable
        };
        context.ExceptionHandled = true;

        return Task.CompletedTask;
    }

    private static bool IsDatabaseWriteFailure(Exception exception) =>
        exception is DbUpdateConcurrencyException
        or DbUpdateException
        or PostgresException;
}
