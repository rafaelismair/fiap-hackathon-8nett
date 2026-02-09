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

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }))
   .WithName("Health");

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
