using Agro.Messaging.Kafka;
using Dashboard.Api;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName));

// Repo Postgres
builder.Services.AddSingleton<IAlertsRepository, AlertsRepository>();

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


app.MapGet("/v1/alerts", async (IAlertsRepository repo, int? take, CancellationToken ct) =>
{
    var t = Math.Clamp(take ?? 50, 1, 500);
    var list = await repo.GetAllAsync(t, ct);
    return Results.Ok(list);
})
.WithName("ListAlerts");

app.MapGet("/v1/alerts/{plotId}", async (IAlertsRepository repo, string plotId, int? take, CancellationToken ct) =>
{
    var t = Math.Clamp(take ?? 50, 1, 500);
    var list = await repo.GetByPlotAsync(plotId, t, ct);
    return Results.Ok(list);
})
.WithName("ListAlertsByPlot");

app.Run();
