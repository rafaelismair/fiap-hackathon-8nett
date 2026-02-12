namespace Ingestion.Api;

public sealed class InfluxOptions
{
    public const string SectionName = "Influx";
    public string Url { get; init; } = default!;
    public string Token { get; init; } = default!;
    public string Org { get; init; } = default!;
    public string Bucket { get; init; } = default!;
}
