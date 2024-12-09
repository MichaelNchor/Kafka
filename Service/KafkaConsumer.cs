using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Nest;

namespace Kafka.Service;

public class KafkaConsumer
{
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IElasticClient _elasticClient;
    private readonly string _topic;
    public KafkaConsumer(IConfiguration configuration, ILogger<KafkaConsumer> logger, IElasticClient elasticClient)
    {
        _logger = logger;
        _topic = configuration["Kafka:TopicName"]!;
        var config = new ConsumerConfig();
        configuration.GetSection("ConsumerConfig").Bind(config);
        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _elasticClient = elasticClient;
    }
    
    public async Task ConsumeAsync(CancellationToken ct = default)
    {
        try
        {
            await Task.Delay(0, ct);

            _consumer.Subscribe(_topic);

            _logger.LogInformation("Consumer started. Press ctrl+c to stop.");

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var message = _consumer.Consume(ct);
                    
                    var res = new KafkaMessage
                    {
                        Id = message.Message.Key,
                        Message = message.Message.Value,
                        Timestamp = DateTime.UtcNow
                    };
                    
                    var indexResponse = await _elasticClient.IndexDocumentAsync(res, ct);

                    if (indexResponse.IsValid)
                    {
                        _logger.LogInformation("Successfully indexed message with ID: {id}", res.Id);
                    }
                    else
                    {
                        _logger.LogError("Failed to index message: {msg}",indexResponse.OriginalException.Message);
                    }

                    _logger.LogInformation("Consumed Message with {key} : {value} @{timestamp}  from topic: {topic}", res.Id,
                        res.Message, DateTime.UtcNow, _topic);
                }
                catch (ConsumeException e)
                {
                    _logger.LogError("Error consuming message: {Error}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException ex)
        {
            _logger.LogError("Consume Cancelled: {ErrorReason}", ex.Message);
        }
        finally
        {
            _consumer.Close();
        }
        
        _logger.LogInformation("Consumer stopped.");
    }
    
    public void Dispose()
    {
        _consumer.Dispose();
    }
    
    private class KafkaMessage
    {
        public string? Id { get; set; }
        public string? Message { get; set; }
        public DateTime Timestamp { get; set; }
    }
}