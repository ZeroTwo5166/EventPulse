using Microsoft.EntityFrameworkCore;

namespace EventPulse.Worker.Ledger;

internal sealed class LedgerDbContext(DbContextOptions<LedgerDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<ProcessedEvent> ProcessedEvents => Set<ProcessedEvent>();
    public DbSet<Notification> Notifications => Set<Notification>();
    public DbSet<DeadLetter> DeadLetters => Set<DeadLetter>();

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

        model.Entity<Notification>(e =>
        {
            e.ToTable("notifications");
            e.HasKey(n => n.Id);
            e.Property(n => n.Id).HasColumnName("id").UseSerialColumn();
            e.Property(n => n.EventId).HasColumnName("event_id");
            e.HasIndex(n => n.EventId).IsUnique();
            e.Property(n => n.OrderId).HasColumnName("order_id");
            e.Property(n => n.Message).HasColumnName("message");
            e.Property(n => n.CreatedAt).HasColumnName("created_at").HasColumnType("timestamptz");
        });

        model.Entity<DeadLetter>(e =>
        {
            e.ToTable("dead_letters");
            e.HasKey(d => d.Id);
            e.Property(d => d.Id).HasColumnName("id").UseSerialColumn();
            e.Property(d => d.EventId).HasColumnName("event_id");
            e.Property(d => d.EventType).HasColumnName("event_type");
            e.Property(d => d.OrderId).HasColumnName("order_id");
            e.Property(d => d.Reason).HasColumnName("reason");
            e.Property(d => d.Detail).HasColumnName("detail");
            e.Property(d => d.Attempts).HasColumnName("attempts");
            e.Property(d => d.RawMessage).HasColumnName("raw_message").HasColumnType("jsonb");
            e.Property(d => d.FailedAt).HasColumnName("failed_at").HasColumnType("timestamptz");
            e.Property(d => d.DlqPublishedAt).HasColumnName("dlq_published_at").HasColumnType("timestamptz");
        });
    }
}
