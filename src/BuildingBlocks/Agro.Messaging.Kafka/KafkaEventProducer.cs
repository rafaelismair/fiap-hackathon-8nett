using System.Text.Json;
using Confluent.Kafka;

namespace Agro.Messaging.Kafka;

public sealed class KafkaEventProducer : IEventProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaEventProducer(IProducer<string, string> producer)
        => _producer = producer;

    public async Task ProduceAsync<T>(string topic, string key, T message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var payload = JsonSerializer.Serialize(message);

        await _producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = payload
        }).ConfigureAwait(false);
    }

    public void Dispose() => _producer.Flush(TimeSpan.FromSeconds(5));
}
