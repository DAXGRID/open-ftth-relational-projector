using Marten;
using Marten.Events.Daemon.Resiliency;
using Marten.Events.Projections;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using OpenFTTH.Events;
using OpenFTTH.EventSourcing;
using OpenFTTH.EventSourcing.Postgres;
using OpenFTTH.RelationalProjector.Database;
using OpenFTTH.RelationalProjector.Settings;
using Serilog;
using Serilog.Formatting.Compact;
using System;
using System.Linq;
using System.Reflection;

namespace OpenFTTH.RelationalProjector
{
    public class Startup
    {
        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            var hostBuilder = new HostBuilder();

            ConfigureApp(hostBuilder);
            ConfigureSerialization(hostBuilder);
            ConfigureLogging(hostBuilder);
            ConfigureServices(hostBuilder);

            return hostBuilder;
        }

        private static void ConfigureApp(IHostBuilder hostBuilder)
        {
            hostBuilder.ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.AddEnvironmentVariables();
                config.AddJsonFile("appsettings.json", true, true);
            });
        }

        private static void ConfigureSerialization(IHostBuilder hostBuilder)
        {
            JsonConvert.DefaultSettings = (() =>
            {
                var settings = new JsonSerializerSettings();
                settings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                settings.Converters.Add(new StringEnumConverter());
                settings.TypeNameHandling = TypeNameHandling.Auto;
                return settings;
            });
        }

        private static void ConfigureLogging(IHostBuilder hostBuilder)
        {
            var loggingConfiguration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, false)
                .AddEnvironmentVariables().Build();

            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddLogging(loggingBuilder =>
                {
                    var logger = new LoggerConfiguration()
                        .ReadFrom.Configuration(loggingConfiguration)
                        .Enrich.FromLogContext()
                        .WriteTo.Console(new CompactJsonFormatter())
                        .CreateLogger();

                    loggingBuilder.AddSerilog(logger, true);
                });
            });
        }

        public static void ConfigureServices(IHostBuilder hostBuilder)
        {


            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddOptions();

                // Settings
                services.Configure<EventStoreDatabaseSetting>(databaseSettings =>
                                hostContext.Configuration.GetSection("EventStoreDatabase").Bind(databaseSettings));

                services.Configure<GeoDatabaseSetting>(databaseSettings =>
                                hostContext.Configuration.GetSection("GeoDatabase").Bind(databaseSettings));

                services.AddSingleton<EventSourcing.IProjection, RelationalDatabaseProjection>();


                // Setup the event store
                services.AddSingleton<IEventStore>(e =>
                        new PostgresEventStore(
                            serviceProvider: e.GetRequiredService<IServiceProvider>(),
                            connectionString: e.GetRequiredService<IOptions<EventStoreDatabaseSetting>>().Value.PostgresConnectionString,
                            databaseSchemaName: "events"
                        ) as IEventStore
                    );


                // Database writer
                services.AddSingleton<PostgresWriter>();
             

                // The worker
                services.AddHostedService<Worker>();
            });
        }
    }
}

