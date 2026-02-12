namespace Dashboard.Api;
public sealed record AlertsSummaryRow(
    string PlotId,
    string Severity,
    int Count
);