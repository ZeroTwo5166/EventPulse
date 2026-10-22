using Microsoft.EntityFrameworkCore;

namespace EventPulse.Worker.Ledger;

internal sealed class LedgerDbContext(DbContextOptions<LedgerDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<Order>(e =>
        {
            e.ToTable("orders");
            e.HasKey(o => o.OrderId);
            e.Property(o => o.OrderId).HasColumnName("order_id");
            e.Property(o => o.CustomerId).HasColumnName("customer_id");
            e.Property(o => o.AmountMinor).HasColumnName("amount_minor");
            e.Property(o => o.Currency).HasColumnName("currency").HasColumnType("char(3)");
            e.Property(o => o.Status).HasColumnName("status");
            e.Property(o => o.Version).HasColumnName("version").HasDefaultValue(1).IsConcurrencyToken();
            e.Property(o => o.UpdatedAt).HasColumnName("updated_at").HasColumnType("timestamptz");
        });

        model.Entity<ProcessedEvent>(e =>
        {
            e.ToTable("processed_events");
            e.HasKey(p => p.EventId);
            // The key must always be the incoming eventId; a client-generated Guid would defeat deduplication.
            e.Property(p => p.EventId).HasColumnName("event_id").ValueGeneratedNever();
            e.Property(p => p.ConsumerGroup).HasColumnName("consumer_group");
            e.Property(p => p.ProcessedAt).HasColumnName("processed_at").HasColumnType("timestamptz");
        });
    }
}
