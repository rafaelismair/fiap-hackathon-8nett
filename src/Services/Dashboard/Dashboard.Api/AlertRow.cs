namespace Dashboard.Api;

public sealed class AlertRow
{
    public string AlertId { get; set; } = default!;
    public string PropertyId { get; set; } = default!;
    public string PlotId { get; set; } = default!;
    public string Rule { get; set; } = default!;
    public string Severity { get; set; } = default!;
    public string Message { get; set; } = default!;
    public DateTimeOffset CreatedAtUtc { get; set; } // TIMESTAMPTZ -> DateTimeOffset
}
