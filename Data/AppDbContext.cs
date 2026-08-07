using DotnetConcurrency.Entities;
using Microsoft.EntityFrameworkCore;

namespace DotnetConcurrency.Data;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Instance> Instances => Set<Instance>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Instance>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Name).HasMaxLength(200).IsRequired();
            entity.Property(x => x.Status).HasMaxLength(100).IsRequired();
            entity.Property(x => x.LastProcessedBy).HasMaxLength(100);
            entity.Property(x => x.Version).IsConcurrencyToken();
        });
    }

    public override int SaveChanges(bool acceptAllChangesOnSuccess)
    {
        ApplyInstanceMetadata();
        return base.SaveChanges(acceptAllChangesOnSuccess);
    }

    public override Task<int> SaveChangesAsync(
        bool acceptAllChangesOnSuccess,
        CancellationToken cancellationToken = default)
    {
        ApplyInstanceMetadata();
        return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
    }

    private void ApplyInstanceMetadata()
    {
        foreach (var entry in ChangeTracker.Entries<Instance>())
        {
            if (entry.State is EntityState.Added or EntityState.Modified)
            {
                entry.Entity.UpdatedAt = DateTimeOffset.UtcNow;
                if (entry.State == EntityState.Modified)
                {
                    entry.Entity.Version++;
                }
            }
        }
    }
}
