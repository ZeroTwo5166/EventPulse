using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace EventPulse.Worker.Migrations;

/// <inheritdoc />
public partial class InitialLedger : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "orders",
            columns: table => new
            {
                order_id = table.Column<string>(type: "text", nullable: false),
                customer_id = table.Column<string>(type: "text", nullable: false),
                amount_minor = table.Column<long>(type: "bigint", nullable: false),
                currency = table.Column<string>(type: "char(3)", nullable: false),
                status = table.Column<string>(type: "text", nullable: false),
                version = table.Column<int>(type: "integer", nullable: false, defaultValue: 1),
                updated_at = table.Column<DateTimeOffset>(type: "timestamptz", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_orders", x => x.order_id);
            });

        migrationBuilder.CreateTable(
            name: "processed_events",
            columns: table => new
            {
                event_id = table.Column<Guid>(type: "uuid", nullable: false),
                consumer_group = table.Column<string>(type: "text", nullable: false),
                processed_at = table.Column<DateTimeOffset>(type: "timestamptz", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_processed_events", x => x.event_id);
            });
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "orders");

        migrationBuilder.DropTable(
            name: "processed_events");
    }
}
