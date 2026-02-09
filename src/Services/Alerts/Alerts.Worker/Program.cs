using Agro.Messaging.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

var builder = Host.CreateApplicationBuilder(args);

// Producer (BuildingBlock)
builder.Services.AddKafkaProducer(builder.Configuration);

// Consumer específico do Worker
builder.Services.AddSingleton<IConsumer<string, string>>(_ =>
{
    var cfg = builder.Configuration;

    var kafka = cfg.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>() ?? new KafkaOptions();
    var groupId = cfg["Worker:ConsumerGroupId"] ?? "alerts-worker-v1";

    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        SessionTimeoutMs = 10000,
        MaxPollIntervalMs = 300000
    };

    var protocol = (kafka.SecurityProtocol ?? "PLAINTEXT").Trim().ToUpperInvariant();

    if (protocol == "SASL_SSL")
    {
        consumerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;

        consumerConfig.SaslMechanism = (kafka.Sasl.Mechanism ?? "SCRAM-SHA-512").Trim().ToUpperInvariant() switch
        {
            "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
            _ => SaslMechanism.ScramSha512
        };

        consumerConfig.SaslUsername = kafka.Sasl.Username;
        consumerConfig.SaslPassword = kafka.Sasl.Password;
    }
    else
    {
        consumerConfig.SecurityProtocol = SecurityProtocol.Plaintext;
    }

    return new ConsumerBuilder<string, string>(consumerConfig).Build();
});

// Hosted service
builder.Services.AddHostedService<Alerts.Worker.SensorReadingToAlertWorker>();

var host = builder.Build();
host.Run();
