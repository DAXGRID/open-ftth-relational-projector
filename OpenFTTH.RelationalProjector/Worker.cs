using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace OpenFTTH.RelationalProjector
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IEventStore _eventStore;

        public Worker(ILogger<Worker> logger, IEventStore eventStore)
        {
            _logger = logger;
            _eventStore = eventStore;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting relational projector worker at: {time}", DateTimeOffset.Now);

            _logger.LogInformation("Start reading all events...");

            await _eventStore.DehydrateProjectionsAsync(stoppingToken).ConfigureAwait(false);

            _logger.LogInformation("Initial event processing finish.");
            _logger.LogInformation("Start listning for new events...");

            File.Create("/tmp/healthy");
            _logger.LogInformation("Healhty file written to tmp.");

            while (!stoppingToken.IsCancellationRequested)
            {
                var eventsProcessed = await _eventStore.CatchUpAsync(stoppingToken).ConfigureAwait(false);

                if (eventsProcessed > 0)
                    _logger.LogInformation($"Processed {eventsProcessed} new events.");

                await Task.Delay(2000, stoppingToken).ConfigureAwait(false);
            }
        }

        public override Task StopAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Stopping background worker");

            return Task.CompletedTask;
        }
    }
}
