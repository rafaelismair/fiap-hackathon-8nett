namespace Agro.Messaging.Kafka;

public interface IEventProducer
{
    Task ProduceAsync<T>(string topic, string key, T message, CancellationToken ct);
}
