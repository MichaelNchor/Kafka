using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Nest;

namespace Kafka;

class Program
{
    private static async Task Main(string[] args)
    {
        using var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                //register services
                services.Configure<ProducerConfig>(context.Configuration.GetSection("ProducerConfig"));
                services.Configure<ConsumerConfig>(context.Configuration.GetSection("ConsumerConfig"));
                services.AddSingleton<KafkaProducer>();
                services.AddSingleton<KafkaConsumer>();
                
                var openSearchUri = context.Configuration["OpenSearch:Uri"];
                var openSearchIndexName = context.Configuration["OpenSearch:IndexName"];

                var settings = new ConnectionSettings(new Uri(openSearchUri!))
                    .DefaultIndex(openSearchIndexName);
                var elasticClient = new ElasticClient(settings);

                services.AddSingleton<IElasticClient>(elasticClient);
            })
            .Build();
        
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        
        var producerTask = ProduceToKafka(host.Services.GetRequiredService<KafkaProducer>());
        var consumerTask = ConsumeFromKafka(host.Services.GetRequiredService<KafkaConsumer>());
        
        await Task.WhenAll(producerTask, consumerTask);
        
        await host.RunAsync(cts.Token);
    }

    private static async Task ProduceToKafka(KafkaProducer producer)
    {
        await producer.ProduceAsync();
    }
    
    private static async Task ConsumeFromKafka(KafkaConsumer producer)
    {
        await producer.ConsumeAsync();
    }
}