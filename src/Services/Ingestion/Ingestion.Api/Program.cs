using Agro.Messaging.Contracts;
using Agro.Messaging.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

// Swagger / OpenAPI
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Kafka Producer
builder.Services.AddKafkaProducer(builder.Configuration);

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }))
   .WithName("Health");

app.MapPost("/v1/readings", async (
    [FromBody] SensorReadingV1 reading,
    IEventProducer producer,
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

    await producer.ProduceAsync(topic, key, reading, ct);

    // FIX do Results.Accepted: ele aceita (string? uri, object? value)
    return Results.Accepted(value: new
    {
        topic,
        key,
        readingId = reading.ReadingId
    });
})
.WithName("IngestReading");

app.Run();
