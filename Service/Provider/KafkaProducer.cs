using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.Service.Provider;

public class KafkaProducer
{
    private ILogger<KafkaProducer> _logger;
    private IProducer<string, string> _producer;
    private string _topic;
    public KafkaProducer(IConfiguration configuration, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        _topic = configuration["Kafka:TopicName"]!;
        var config = new ProducerConfig();
        configuration.GetSection("ProducerConfig").Bind(config);
        _producer = new ProducerBuilder<string, string>(config).Build();
    }
    
    public async Task ProduceAsync(CancellationToken ct = default)
    {
        try
        {
            _logger.LogInformation("Producer started. Press ctrl+c to stop.");

            int counter = 1;
            while (!ct.IsCancellationRequested)
            {
                var key = $"id-{counter}";
                var value = $"message {counter} @{DateTime.UtcNow}";
                var deliveryResult = await _producer.ProduceAsync(_topic, new Message<string, string>
                {
                    Key = key,
                    Value = value
                },ct);

                _logger.LogInformation("Produced Message {key} : {value} delivered to {topic}",
                    key, value, deliveryResult.Topic);

                counter++;
                await Task.Delay(2000, ct);
            }
            
            _logger.LogInformation("Consumer Stopped");
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError("Produce failed: {ErrorReason}", ex.Error.Reason);
        }
    }
    
    public void Dispose()
    {
        _producer.Dispose();
    }
}