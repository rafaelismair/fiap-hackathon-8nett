using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Agro.Messaging.Kafka;

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaProducer(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "Kafka:BootstrapServers é obrigatório.");

        services.AddSingleton<IProducer<string, string>>(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;

            var protocol = (opts.SecurityProtocol ?? "PLAINTEXT").Trim().ToUpperInvariant();

            var cfg = new ProducerConfig
            {
                BootstrapServers = opts.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 200
            };

            if (protocol == "SASL_SSL")
            {
                var mechanism = (opts.Sasl.Mechanism ?? "SCRAM-SHA-512").Trim().ToUpperInvariant() switch
                {
                    "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
                    _ => SaslMechanism.ScramSha512
                };

                cfg.SecurityProtocol = SecurityProtocol.SaslSsl;
                cfg.SaslMechanism = mechanism;
                cfg.SaslUsername = opts.Sasl.Username;
                cfg.SaslPassword = opts.Sasl.Password;
            }
            else
            {
                cfg.SecurityProtocol = SecurityProtocol.Plaintext;
            }

            return new ProducerBuilder<string, string>(cfg).Build();
        });

        services.AddSingleton<IEventProducer, KafkaEventProducer>();
        return services;
    }
}
