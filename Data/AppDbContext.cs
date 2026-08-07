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

    public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
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

        return base.SaveChangesAsync(cancellationToken);
    }
}
