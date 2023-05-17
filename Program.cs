using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;



var configuration = new ConfigurationBuilder()
     .AddJsonFile($"local.settings.json");

var config = configuration.Build();

var numOfEvents = config.GetValue<int>("numOfEvents");
var numOfBatches = config.GetValue<int>("numOfBatches");
var eventHubNamespace = config.GetValue<string>("eventHubNamespace");
var eventHubName = config.GetValue<string>("eventHubName");


// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
// of the application, which is best practice when events are being published or read regularly.
EventHubProducerClient producerClient = new EventHubProducerClient(
    eventHubNamespace,
    eventHubName,
    new DefaultAzureCredential());

Stopwatch stopwatch = Stopwatch.StartNew();

// create i batches of j events each
try
{
    for (int i = 0; i < numOfBatches; i++)
    {
        // Create a batch of events 
        EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

        for (int j = 0; j < numOfEvents; j++)
        {
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Batch {i} --- Event {j}"))))
            {
                // if it is too large for the batch
                throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
            }
        }
        Task sendTask = producerClient.SendAsync(eventBatch);
        sendTask.Wait();
        if (sendTask.Exception != null)
            Console.WriteLine(sendTask.Exception.Message);

        Console.WriteLine($"A batch number {i} with {numOfEvents} events has been published.");
        eventBatch.Dispose();
    }
}
finally
{
    await producerClient.DisposeAsync();
}
stopwatch.Stop();
Console.WriteLine($"It took {stopwatch.ElapsedMilliseconds} milliseconds to send {numOfEvents * numOfBatches} events.");
// calculate the numOfEvents * numOfBatches / stopwatch.ElapsedMilliseconds as double with two decimal places

Console.WriteLine($"The throughput per millisecond is {Math.Round((double)numOfEvents * numOfBatches / stopwatch.ElapsedMilliseconds, 2)} events per millisecond.");
// calculate the numOfEvents * numOfBatches / stopwatch.ElapsedMilliseconds * 1000 as double with two decimal places
Console.WriteLine($"The throughput per second is {Math.Round((double)numOfEvents * numOfBatches / stopwatch.ElapsedMilliseconds * 1000, 2)} events per second.");