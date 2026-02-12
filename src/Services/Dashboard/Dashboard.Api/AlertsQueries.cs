using Dapper;
using Npgsql;
namespace Dashboard.Api;

public interface IAlertsQueries
{
    Task<IReadOnlyList<AlertRow>> GetLatestAsync(int take, CancellationToken ct);
    Task<IReadOnlyList<AlertsSummaryRow>> GetSummaryLastMinutesAsync(int minutes, CancellationToken ct);
}

public sealed class AlertsQueries : IAlertsQueries
{
    private readonly string _connString;

    public AlertsQueries(IConfiguration configuration)
    {
        _connString = configuration.GetConnectionString("Postgres")
            ?? throw new InvalidOperationException("ConnectionStrings:Postgres não configurado.");
    }

    private NpgsqlConnection Open() => new(_connString);

    public async Task<IReadOnlyList<AlertRow>> GetLatestAsync(int take, CancellationToken ct)
    {
        const string sql = """
        SELECT alert_id    AS "AlertId",
               property_id AS "PropertyId",
               plot_id     AS "PlotId",
               rule        AS "Rule",
               severity    AS "Severity",
               message     AS "Message",
               created_at_utc AS "CreatedAtUtc"
          FROM alerts
         ORDER BY created_at_utc DESC
         LIMIT @Take;
        """;

        await using var conn = Open();
        var rows = await conn.QueryAsync<AlertRow>(new CommandDefinition(sql, new { Take = take }, cancellationToken: ct));
        return rows.ToList();
    }

    public async Task<IReadOnlyList<AlertsSummaryRow>> GetSummaryLastMinutesAsync(int minutes, CancellationToken ct)
    {
        const string sql = """
        SELECT plot_id AS "PlotId",
               severity AS "Severity",
               COUNT(*)::int AS "Count"
          FROM alerts
         WHERE created_at_utc >= (NOW() AT TIME ZONE 'UTC') - (@Minutes || ' minutes')::interval
         GROUP BY plot_id, severity
         ORDER BY plot_id, severity;
        """;

        await using var conn = Open();
        var rows = await conn.QueryAsync<AlertsSummaryRow>(new CommandDefinition(sql, new { Minutes = minutes }, cancellationToken: ct));
        return rows.ToList();
    }
}
