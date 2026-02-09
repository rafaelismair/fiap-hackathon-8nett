namespace Agro.Messaging.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = string.Empty;

    public KafkaSaslOptions Sasl { get; init; } = new();

    public KafkaTopicsOptions Topics { get; init; } = new();

    public string SecurityProtocol { get; init; } = "PLAINTEXT"; 

}

public sealed class KafkaSaslOptions
{
    public string Username { get; init; } = string.Empty;
    public string Password { get; init; } = string.Empty;
    public string Mechanism { get; init; } = "SCRAM-SHA-512"; // SCRAM-SHA-256 | SCRAM-SHA-512
}

public sealed class KafkaTopicsOptions
{
    public string SensorReadings { get; init; } = "sensor.readings.v1";
    public string AlertsCreated { get; init; } = "alerts.created.v1";
}


