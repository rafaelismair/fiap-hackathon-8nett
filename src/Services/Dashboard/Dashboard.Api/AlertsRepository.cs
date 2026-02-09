using System.Data;
using Agro.Messaging.Contracts;
using Dapper;
using Npgsql;

namespace Dashboard.Api;

public interface IAlertsRepository
{
    Task InsertAsync(AlertCreatedV1 alert, CancellationToken ct);
    Task<IReadOnlyList<AlertCreatedV1>> GetAllAsync(int take, CancellationToken ct);
    Task<IReadOnlyList<AlertCreatedV1>> GetByPlotAsync(string plotId, int take, CancellationToken ct);
}

public sealed class AlertsRepository : IAlertsRepository
{
    private readonly string _connString;

    public AlertsRepository(IConfiguration configuration)
    {
        _connString = configuration.GetConnectionString("Postgres")
                      ?? throw new InvalidOperationException("ConnectionStrings:Postgres não configurado.");
    }

    private IDbConnection CreateConnection() => new NpgsqlConnection(_connString);

    public async Task InsertAsync(AlertCreatedV1 alert, CancellationToken ct)
    {
        const string sql = """
        INSERT INTO alerts (alert_id, property_id, plot_id, rule, severity, message, created_at_utc)
        VALUES (@AlertId, @PropertyId, @PlotId, @Rule, @Severity, @Message, @CreatedAtUtc)
        ON CONFLICT (alert_id) DO NOTHING;
        """;

        using var conn = CreateConnection();
        await conn.ExecuteAsync(new CommandDefinition(sql, alert, cancellationToken: ct));
    }

public async Task<IReadOnlyList<AlertCreatedV1>> GetAllAsync(int take, CancellationToken ct)
{
    const string sql = """
    SELECT alert_id        AS "AlertId",
           property_id     AS "PropertyId",
           plot_id         AS "PlotId",
           rule            AS "Rule",
           severity        AS "Severity",
           message         AS "Message",
           created_at_utc  AS "CreatedAtUtc"
      FROM alerts
     ORDER BY created_at_utc DESC
     LIMIT @Take;
    """;

    using var conn = CreateConnection();
    var rows = await conn.QueryAsync<AlertRow>(
        new CommandDefinition(sql, new { Take = take }, cancellationToken: ct));

    return rows.Select(r => new AlertCreatedV1(
        r.AlertId,
        r.PropertyId,
        r.PlotId,
        r.Rule,
        r.Severity,
        r.Message,
        r.CreatedAtUtc
    )).ToList();
}

public async Task<IReadOnlyList<AlertCreatedV1>> GetByPlotAsync(string plotId, int take, CancellationToken ct)
{
    const string sql = """
    SELECT alert_id        AS "AlertId",
           property_id     AS "PropertyId",
           plot_id         AS "PlotId",
           rule            AS "Rule",
           severity        AS "Severity",
           message         AS "Message",
           created_at_utc  AS "CreatedAtUtc"
      FROM alerts
     WHERE plot_id = @PlotId
     ORDER BY created_at_utc DESC
     LIMIT @Take;
    """;

    using var conn = CreateConnection();
    var rows = await conn.QueryAsync<AlertRow>(
        new CommandDefinition(sql, new { PlotId = plotId, Take = take }, cancellationToken: ct));

    return rows.Select(r => new AlertCreatedV1(
        r.AlertId,
        r.PropertyId,
        r.PlotId,
        r.Rule,
        r.Severity,
        r.Message,
        r.CreatedAtUtc
    )).ToList();
}

}
