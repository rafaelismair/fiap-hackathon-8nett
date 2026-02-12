using Agro.Messaging.Kafka;
using Dashboard.Api;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName));

// Repo Postgres
builder.Services.AddSingleton<IAlertsRepository, AlertsRepository>();

builder.Services.AddSingleton<IAlertsQueries, AlertsQueries>();

// Consumer do tópico (persistindo)
builder.Services.AddHostedService<AlertsConsumerHostedService>();

builder.Services.AddOptions<InfluxOptions>()
    .Bind(builder.Configuration.GetSection(InfluxOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.Url), "Influx:Url é obrigatório.");

builder.Services.AddSingleton<IInfluxReader, InfluxReader>();


var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }))
   .WithName("Health");

app.MapGet("/v1/plots/{plotId}/metrics/{metric}", async (
    string plotId,
    string metric,
    string? from,
    string? to,
    int? limit,
    IInfluxReader influx,
    CancellationToken ct) =>
{
    // defaults: últimas 24h
    var toUtc = string.IsNullOrWhiteSpace(to)
        ? DateTimeOffset.UtcNow
        : DateTimeOffset.Parse(to);

    var fromUtc = string.IsNullOrWhiteSpace(from)
        ? toUtc.AddHours(-24)
        : DateTimeOffset.Parse(from);

    var lim = Math.Clamp(limit ?? 500, 1, 5000);

    var points = await influx.QueryMetricAsync(plotId, metric, fromUtc, toUtc, lim, ct);
    return Results.Ok(new
    {
        plotId,
        metric,
        fromUtc,
        toUtc,
        count = points.Count,
        points
    });
})
.WithName("GetMetricSeries");



app.MapGet("/v1/alerts/{plotId}", async (IAlertsRepository repo, string plotId, int? take, CancellationToken ct) =>
{
    var t = Math.Clamp(take ?? 50, 1, 500);
    var list = await repo.GetByPlotAsync(plotId, t, ct);
    return Results.Ok(list);
})
.WithName("ListAlertsByPlot");

app.MapGet("/v1/alerts", async (int? take, IAlertsQueries q, CancellationToken ct) =>
{
    var t = Math.Clamp(take ?? 200, 1, 2000);
    var rows = await q.GetLatestAsync(t, ct);
    return Results.Ok(new { count = rows.Count, items = rows });
})
.WithName("GetAlerts");

app.MapGet("/v1/alerts/summary", async (int? minutes, IAlertsQueries q, CancellationToken ct) =>
{
    var m = Math.Clamp(minutes ?? 60, 1, 24 * 60);
    var rows = await q.GetSummaryLastMinutesAsync(m, ct);

    // formato amigável pro front: plot -> severities -> count
    var grouped = rows
        .GroupBy(x => x.PlotId)
        .ToDictionary(
            g => g.Key,
            g => g.ToDictionary(x => x.Severity, x => x.Count)
        );

    return Results.Ok(new { minutes = m, plots = grouped });
})
.WithName("GetAlertsSummary");

app.MapGet("/v1/plots/{plotId}/metrics/{metric}/agg", async (
    string plotId,
    string metric,
    string? window,
    int? hours,
    int? limit,
    IInfluxReader influx,
    CancellationToken ct) =>
{
    var w = string.IsNullOrWhiteSpace(window) ? "5m" : window;
    var h = Math.Clamp(hours ?? 24, 1, 24 * 30);
    var lim = Math.Clamp(limit ?? 5000, 1, 20000);

    var toUtc = DateTimeOffset.UtcNow;
    var fromUtc = toUtc.AddHours(-h);

    var points = await influx.QueryMetricAggAsync(plotId, metric, fromUtc, toUtc, w, lim, ct);
    return Results.Ok(new { plotId, metric, window = w, fromUtc, toUtc, count = points.Count, points });
})
.WithName("GetMetricAgg");


app.Run();
