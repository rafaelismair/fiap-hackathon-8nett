namespace Dashboard.Api;

public sealed record MetricPointDto(DateTimeOffset TimestampUtc, double Value);
