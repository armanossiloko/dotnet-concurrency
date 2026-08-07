using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace DotnetConcurrency.Infrastructure;

/// <summary>
/// Converts only retryable database failures into a stable API response.
/// Constraint and programming errors are intentionally left unhandled.
/// </summary>
public sealed class TransientDatabaseExceptionFilter(
    ILogger<TransientDatabaseExceptionFilter> logger) : IAsyncExceptionFilter
{
    public Task OnExceptionAsync(ExceptionContext context)
    {
        if (!TransientDatabaseErrors.IsRetryable(context.Exception))
        {
            return Task.CompletedTask;
        }

        logger.LogWarning(
            context.Exception,
            "Transient database operation failed for {Path} after provider retries",
            context.HttpContext.Request.Path);

        context.Result = new ObjectResult(new ProblemDetails
        {
            Status = StatusCodes.Status503ServiceUnavailable,
            Title = "The database is temporarily unavailable",
            Detail = "Please retry the request."
        })
        {
            StatusCode = StatusCodes.Status503ServiceUnavailable
        };
        context.ExceptionHandled = true;

        return Task.CompletedTask;
    }

}

public static class TransientDatabaseErrors
{
    public static bool IsRetryable(Exception exception)
    {
        if (exception is DbUpdateConcurrencyException)
        {
            return true;
        }

        var databaseException = exception switch
        {
            DbUpdateException updateException => updateException.InnerException,
            _ => exception
        };

        return databaseException switch
        {
            PostgresException postgresException =>
                postgresException.SqlState is
                    PostgresErrorCodes.SerializationFailure or
                    PostgresErrorCodes.DeadlockDetected,
            NpgsqlException npgsqlException => npgsqlException.IsTransient,
            _ => false
        };
    }
}
