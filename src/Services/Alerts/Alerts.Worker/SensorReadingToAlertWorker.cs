using System.Text.Json;
using Agro.Messaging.Contracts;
using Agro.Messaging.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Alerts.Worker;

public sealed class SensorReadingToAlertWorker : BackgroundService
{
    private readonly ILogger<SensorReadingToAlertWorker> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IEventProducer _producer;
    private readonly IOptions<KafkaOptions> _kafkaOptions;

    public SensorReadingToAlertWorker(
        ILogger<SensorReadingToAlertWorker> logger,
        IConsumer<string, string> consumer,
        IEventProducer producer,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        _consumer = consumer;
        _producer = producer;
        _kafkaOptions = kafkaOptions;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken ct)
    {
        var inputTopic = _kafkaOptions.Value.Topics.SensorReadings;
        var outputTopic = _kafkaOptions.Value.Topics.AlertsCreated;

        _consumer.Subscribe(inputTopic);

        _logger.LogInformation("Alerts.Worker iniciado. Consumindo tópico: {Topic}", inputTopic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string>? cr = null;

                try
                {
                    cr = _consumer.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Erro consumindo mensagem Kafka");
                    continue;
                }

                if (cr?.Message?.Value is null)
                    continue;

                SensorReadingV1? reading;

                try
                {
                    reading = JsonSerializer.Deserialize<SensorReadingV1>(cr.Message.Value);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Falha ao desserializar SensorReadingV1. Payload: {Payload}", cr.Message.Value);
                    _consumer.Commit(cr);
                    continue;
                }

                if (reading is null)
                {
                    _consumer.Commit(cr);
                    continue;
                }

                if (ShouldGenerateAlert(reading, out var alert))
                {
                    try
                    {
                        _producer.ProduceAsync(outputTopic, key: reading.PlotId, message: alert!, ct)
                                 .GetAwaiter().GetResult();

                        _logger.LogInformation(
                            "ALERTA GERADO: plot={PlotId} metric={Metric} value={Value}{Unit} -> alertId={AlertId}",
                            reading.PlotId, reading.Metric, reading.Value, reading.Unit, alert!.AlertId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Falha ao publicar alert no tópico {Topic}", outputTopic);
                        continue; // não commita pra tentar reprocessar
                    }
                }
                else
                {
                    _logger.LogInformation(
                        "Leitura ok: plot={PlotId} metric={Metric} value={Value}{Unit}",
                        reading.PlotId, reading.Metric, reading.Value, reading.Unit);
                }

                _consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Cancelamento solicitado. Encerrando consumo...");
        }
        finally
        {
            _consumer.Close();
        }
    }

    private static bool ShouldGenerateAlert(SensorReadingV1 reading, out AlertCreatedV1? alert)
    {
        alert = null;

        if (!string.Equals(reading.Metric, "soil_moisture", StringComparison.OrdinalIgnoreCase))
            return false;

        if (reading.Value >= 30)
            return false;

        alert = new AlertCreatedV1(
            AlertId: Guid.NewGuid().ToString("N"),
            PropertyId: reading.PropertyId,
            PlotId: reading.PlotId,
            Rule: "soil_moisture_below_30",
            Severity: "high",
            Message: $"Umidade do solo baixa ({reading.Value}{reading.Unit})",
            CreatedAtUtc: DateTimeOffset.UtcNow
        );

        return true;
    }
}
