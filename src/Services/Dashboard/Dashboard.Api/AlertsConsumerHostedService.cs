using System.Text.Json;
using Agro.Messaging.Contracts;
using Agro.Messaging.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Dashboard.Api;

public sealed class AlertsConsumerHostedService : BackgroundService
{
    private readonly ILogger<AlertsConsumerHostedService> _logger;
    private readonly IConfiguration _config;
    private readonly IOptions<KafkaOptions> _kafkaOptions;
    private readonly IAlertsRepository _repo;

    public AlertsConsumerHostedService(
        ILogger<AlertsConsumerHostedService> logger,
        IConfiguration config,
        IOptions<KafkaOptions> kafkaOptions,
        IAlertsRepository repo)
    {
        _logger = logger;
        _config = config;
        _kafkaOptions = kafkaOptions;
        _repo = repo;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken ct)
    {
        var kafka = _kafkaOptions.Value;
        var topic = kafka.Topics.AlertsCreated;
        var groupId = _config["Dashboard:ConsumerGroupId"] ?? "dashboard-api-v1";

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

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("Dashboard.Api consumindo tópico: {Topic} (groupId={GroupId})", topic, groupId);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string> cr;
                try
                {
                    cr = consumer.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Erro consumindo Kafka");
                    continue;
                }

                if (cr?.Message?.Value is null)
                    continue;

                AlertCreatedV1? alert;
                try
                {
                    alert = JsonSerializer.Deserialize<AlertCreatedV1>(cr.Message.Value);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Falha ao desserializar AlertCreatedV1. Payload: {Payload}", cr.Message.Value);
                    consumer.Commit(cr);
                    continue;
                }

                if (alert is not null)
                {
                    try
                    {
                        _repo.InsertAsync(alert, ct).GetAwaiter().GetResult();

                        _logger.LogInformation(
                            "ALERTA persistido: plot={PlotId} severity={Severity} rule={Rule} alertId={AlertId}",
                            alert.PlotId, alert.Severity, alert.Rule, alert.AlertId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Falha ao persistir alerta no Postgres (vai reprocessar). alertId={AlertId}", alert.AlertId);
                        continue; // não commita
                    }
                }

                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Dashboard.Api: cancelamento solicitado.");
        }
        finally
        {
            consumer.Close();
        }
    }
}
