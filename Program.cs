using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;



var configuration =  new ConfigurationBuilder()
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

// create a list of event batches which we will send to the event hub
List<EventDataBatch> eventBatches = new List<EventDataBatch>();

// create 20 batches of 2000 events each
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
    eventBatches.Add(eventBatch);
}

try
{
    // measure the time it takes to send the events to the event hub
    Stopwatch stopwatch = Stopwatch.StartNew();
    // iterate over the list of batches and send the events to the event hub
    foreach (EventDataBatch eventBatch in eventBatches)
    {
        // Use the producer client to send the batch of events to the event hub
        await producerClient.SendAsync(eventBatch);
        Console.WriteLine($"A batch number {eventBatches.IndexOf(eventBatch)} with {numOfEvents} events has been published.");
    }
    stopwatch.Stop();
    Console.WriteLine($"It took {stopwatch.ElapsedMilliseconds} milliseconds to send {numOfEvents * numOfBatches} events.");
}
finally
{
    await producerClient.DisposeAsync();
}