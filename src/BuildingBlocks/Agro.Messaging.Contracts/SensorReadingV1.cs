namespace Agro.Messaging.Contracts;

/// <summary>
/// Evento publicado quando uma leitura de sensor é recebida.
/// </summary>
public sealed record SensorReadingV1(
    string ReadingId,
    string ProducerId,
    string PropertyId,
    string PlotId,
    string SensorId,
    string Metric,              // ex: "soil_moisture", "temperature", etc.
    decimal Value,
    string Unit,                // ex: "%", "C"
    DateTimeOffset TimestampUtc
);
