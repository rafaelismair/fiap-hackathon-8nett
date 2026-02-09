using Agro.Messaging.Contracts;
using System.Collections.Concurrent;

namespace Dashboard.Api;

public interface IAlertStore
{
    void Add(AlertCreatedV1 alert);
    IReadOnlyList<AlertCreatedV1> GetAll(int? take = null);
    IReadOnlyList<AlertCreatedV1> GetByPlot(string plotId, int? take = null);
}

public sealed class InMemoryAlertStore : IAlertStore
{
    private readonly ConcurrentQueue<AlertCreatedV1> _queue = new();
    private readonly int _max;

    public InMemoryAlertStore(int maxAlertsInMemory)
    {
        _max = Math.Max(10, maxAlertsInMemory);
    }

    public void Add(AlertCreatedV1 alert)
    {
        _queue.Enqueue(alert);

        // mantém só os últimos N (simples e suficiente pra MVP)
        while (_queue.Count > _max && _queue.TryDequeue(out _))
        {
        }
    }

    public IReadOnlyList<AlertCreatedV1> GetAll(int? take = null)
    {
        var list = _queue.ToArray()
            .OrderByDescending(a => a.CreatedAtUtc)
            .ToList();

        if (take is > 0) list = list.Take(take.Value).ToList();
        return list;
    }

    public IReadOnlyList<AlertCreatedV1> GetByPlot(string plotId, int? take = null)
    {
        var list = _queue.ToArray()
            .Where(a => string.Equals(a.PlotId, plotId, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(a => a.CreatedAtUtc)
            .ToList();

        if (take is > 0) list = list.Take(take.Value).ToList();
        return list;
    }
}
