using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace EventPulse.Worker.Migrations;

/// <inheritdoc />
public partial class AddDeadLetters : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "dead_letters",
            columns: table => new
            {
                id = table.Column<long>(type: "bigint", nullable: false)
                    .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                event_id = table.Column<Guid>(type: "uuid", nullable: true),
                event_type = table.Column<string>(type: "text", nullable: true),
                order_id = table.Column<string>(type: "text", nullable: true),
                reason = table.Column<string>(type: "text", nullable: false),
                detail = table.Column<string>(type: "text", nullable: false),
                attempts = table.Column<int>(type: "integer", nullable: false),
                raw_message = table.Column<string>(type: "jsonb", nullable: false),
                failed_at = table.Column<DateTimeOffset>(type: "timestamptz", nullable: false),
                dlq_published_at = table.Column<DateTimeOffset>(type: "timestamptz", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_dead_letters", x => x.id);
            });
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "dead_letters");
    }
}
