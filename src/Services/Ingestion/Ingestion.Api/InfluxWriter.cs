using InfluxDB.Client;
using InfluxDB.Client.Writes;
using InfluxDB.Client.Api.Domain;

namespace Ingestion.Api;

public interface IInfluxWriter
{
    Task WriteReadingAsync(
        string propertyId,
        string plotId,
        string sensorId,
        string metric,
        double value,
        string unit,
        DateTimeOffset timestampUtc,
        CancellationToken ct);
}

public sealed class InfluxWriter : IInfluxWriter
{
    private readonly InfluxDBClient _client;
    private readonly InfluxOptions _opts;

    public InfluxWriter(InfluxOptions opts)
    {
        _opts = opts;

        // Forma recomendada (sem o Create deprecated)
        _client = new InfluxDBClient(opts.Url, opts.Token);
    }

    public async Task WriteReadingAsync(
        string propertyId,
        string plotId,
        string sensorId,
        string metric,
        double value,
        string unit,
        DateTimeOffset timestampUtc,
        CancellationToken ct)
    {
        var write = _client.GetWriteApiAsync(); // sem using

        var point = PointData
            .Measurement("sensor_readings")
            .Tag("propertyId", propertyId)
            .Tag("plotId", plotId)
            .Tag("sensorId", sensorId)
            .Tag("metric", metric)
            .Tag("unit", unit)
            .Field("value", value)
            .Timestamp(timestampUtc.UtcDateTime, WritePrecision.Ns);

        await write.WritePointAsync(point, _opts.Bucket, _opts.Org, ct);
    }
}
