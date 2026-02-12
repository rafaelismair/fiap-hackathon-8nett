using InfluxDB.Client;
using InfluxDB.Client.Core.Flux.Domain;
using Microsoft.Extensions.Options;

namespace Dashboard.Api;

public interface IInfluxReader
{
    Task<IReadOnlyList<MetricPointDto>> QueryMetricAsync(
        string plotId,
        string metric,
        DateTimeOffset fromUtc,
        DateTimeOffset toUtc,
        int limit,
        CancellationToken ct);

        Task<IReadOnlyList<MetricPointDto>> QueryMetricAggAsync(
            string plotId,
            string metric,
            DateTimeOffset fromUtc,
            DateTimeOffset toUtc,
            string window,
            int limit,
            CancellationToken ct);
}

public sealed class InfluxReader : IInfluxReader
{
    private readonly InfluxDBClient _client;
    private readonly InfluxOptions _opts;

    public InfluxReader(IOptions<InfluxOptions> options)
    {
        _opts = options.Value;
        _client = new InfluxDBClient(_opts.Url, _opts.Token);
    }

    public async Task<IReadOnlyList<MetricPointDto>> QueryMetricAsync(
        string plotId,
        string metric,
        DateTimeOffset fromUtc,
        DateTimeOffset toUtc,
        int limit,
        CancellationToken ct)
    {
        // Query Flux:
        // - measurement: sensor_readings
        // - tags: plotId, metric
        // - field: value
        var flux = $"""
        from(bucket: "{_opts.Bucket}")
          |> range(start: {fromUtc.UtcDateTime:o}, stop: {toUtc.UtcDateTime:o})
          |> filter(fn: (r) => r._measurement == "sensor_readings")
          |> filter(fn: (r) => r._field == "value")
          |> filter(fn: (r) => r.plotId == "{Escape(plotId)}")
          |> filter(fn: (r) => r.metric == "{Escape(metric)}")
          |> keep(columns: ["_time", "_value"])
          |> sort(columns: ["_time"], desc: false)
          |> limit(n: {limit})
        """;

        var queryApi = _client.GetQueryApi();
        var tables = await queryApi.QueryAsync(flux, _opts.Org, ct);

        var points = new List<MetricPointDto>();

        foreach (var table in tables)
        {
            foreach (var record in table.Records)
            {
                var instant = record.GetTime();
                var valueObj = record.GetValue();

                if (instant is null || valueObj is null)
                    continue;

                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(
                    instant.Value.ToUnixTimeMilliseconds()
                );

                var value = valueObj switch
                {
                    double d => d,
                    float f => f,
                    long l => l,
                    int i => i,
                    decimal dec => (double)dec,
                    _ => Convert.ToDouble(valueObj)
                };

                points.Add(new MetricPointDto(timestamp, value));
            }
        }


        return points;
    }

    public async Task<IReadOnlyList<MetricPointDto>> QueryMetricAggAsync(
    string plotId,
    string metric,
    DateTimeOffset fromUtc,
    DateTimeOffset toUtc,
    string window,
    int limit,
    CancellationToken ct)
    {
        var flux = $"""
        from(bucket: "{_opts.Bucket}")
        |> range(start: {fromUtc.UtcDateTime:o}, stop: {toUtc.UtcDateTime:o})
        |> filter(fn: (r) => r._measurement == "sensor_readings")
        |> filter(fn: (r) => r._field == "value")
        |> filter(fn: (r) => r.plotId == "{Escape(plotId)}")
        |> filter(fn: (r) => r.metric == "{Escape(metric)}")
        |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)
        |> keep(columns: ["_time", "_value"])
        |> sort(columns: ["_time"], desc: false)
        |> limit(n: {limit})
        """;

        var queryApi = _client.GetQueryApi();
        var tables = await queryApi.QueryAsync(flux, _opts.Org, ct);

        var points = new List<MetricPointDto>();

        foreach (var table in tables)
        {
            foreach (var record in table.Records)
            {
                var instant = record.GetTime();
                var valueObj = record.GetValue();

                if (instant is null || valueObj is null)
                    continue;

                var ts = DateTimeOffset.FromUnixTimeMilliseconds(instant.Value.ToUnixTimeMilliseconds());
                var value = Convert.ToDouble(valueObj);

                points.Add(new MetricPointDto(ts, value));
            }
        }

        return points;
    }


    private static string Escape(string s)
        => s.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
