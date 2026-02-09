namespace Agro.Messaging.Contracts;

/// <summary>
/// Evento publicado quando um alerta é gerado.
/// </summary>
public sealed record AlertCreatedV1(
    string AlertId,
    string PropertyId,
    string PlotId,
    string Rule,               // ex: "soil_moisture_below_30"
    string Severity,           // ex: "low|medium|high"
    string Message,
    DateTimeOffset CreatedAtUtc
);
