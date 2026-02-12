using Agro.Messaging.Contracts;
using Agro.Messaging.Kafka;
using Ingestion.Api;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddKafkaProducer(builder.Configuration);

builder.Services.AddOptions<InfluxOptions>()
    .Bind(builder.Configuration.GetSection(InfluxOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.Url), "Influx:Url é obrigatório");

builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<InfluxOptions>>().Value);

builder.Services.AddSingleton<IInfluxWriter, InfluxWriter>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }))
   .WithName("Health");

app.MapPost("/v1/readings", async (
    [FromBody] SensorReadingV1 reading,
    IEventProducer producer,
    IInfluxWriter influx,
    IOptions<KafkaOptions> kafkaOptions,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(reading.ReadingId))
        return Results.BadRequest("ReadingId é obrigatório.");
    if (string.IsNullOrWhiteSpace(reading.PropertyId))
        return Results.BadRequest("PropertyId é obrigatório.");
    if (string.IsNullOrWhiteSpace(reading.PlotId))
        return Results.BadRequest("PlotId é obrigatório.");
    if (string.IsNullOrWhiteSpace(reading.Metric))
        return Results.BadRequest("Metric é obrigatório.");

    var topic = kafkaOptions.Value.Topics.SensorReadings;
    var key = reading.PlotId;

    // 1) Publica no Kafka
    await producer.ProduceAsync(topic, key, reading, ct);

    // 2) Persiste no InfluxDB (time-series)
    await influx.WriteReadingAsync(
        propertyId: reading.PropertyId,
        plotId: reading.PlotId,
        sensorId: reading.SensorId,
        metric: reading.Metric,
        value: Convert.ToDouble(reading.Value),
        unit: reading.Unit,
        timestampUtc: reading.TimestampUtc,
        ct: ct);

    return Results.Accepted(value: new
    {
        topic,
        key,
        readingId = reading.ReadingId
    });
})
.WithName("IngestReading");

app.Run();
