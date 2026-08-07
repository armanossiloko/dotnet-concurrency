using DotnetConcurrency.Concurrency;
using DotnetConcurrency.Data;
using DotnetConcurrency.Entities;
using DotnetConcurrency.Infrastructure;
using DotnetConcurrency.Workers;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers(options =>
    options.Filters.Add<DatabaseWriteExceptionFilter>());
builder.Services.AddOpenApi();

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("Default"),
        npgsql => npgsql.EnableRetryOnFailure(maxRetryCount: 3)));

builder.Services.AddSingleton<InstanceGate>();

builder.Services.AddHostedService<CleanupWorker>();
builder.Services.AddHostedService<MetricsWorker>();
builder.Services.AddHostedService<SyncWorker>();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.MigrateAsync();

    if (!await db.Instances.AnyAsync())
    {
        db.Instances.AddRange(
            new Instance { Id = Guid.Parse("11111111-1111-1111-1111-111111111111"), Name = "alpha", Status = "Idle" },
            new Instance { Id = Guid.Parse("22222222-2222-2222-2222-222222222222"), Name = "beta", Status = "Idle" },
            new Instance { Id = Guid.Parse("33333333-3333-3333-3333-333333333333"), Name = "gamma", Status = "Idle" });
        await db.SaveChangesAsync();
    }
}

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapControllers();

app.Run();
