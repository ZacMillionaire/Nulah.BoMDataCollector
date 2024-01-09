using System;
using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Nulah.BoMDataCollector.App.Controllers;
using Nulah.BoMDataCollector.App.Models;
using Nulah.BoMDataCollector.App.Services;

namespace Nulah.BoMDataCollector.App;

class Program
{
	static async Task Main(string[] args)
	{
		Console.WriteLine("Hello, World!");

		var host = Host.CreateDefaultBuilder()
			.ConfigureServices(ConfigureServices)
			.Build();

		await host.StartAsync();

		await host.WaitForShutdownAsync();
	}

	private static void ConfigureServices(HostBuilderContext hostContext, IServiceCollection serviceCollection)
	{
		serviceCollection.Configure<Configuration>(options => hostContext.Configuration.Bind(options));
		serviceCollection.AddSingleton<BoMCollector>();
		serviceCollection.AddHostedService<CollectData>();
		serviceCollection.AddSingleton<InfluxDBClient>(_ =>
			{
				var config = hostContext.Configuration.Get<Configuration>();

				if (config == null)
				{
					throw new Exception("Configuration is impossibly null. Have you neglected to have an appsettings.json file?");
				}

				return new InfluxDBClient(config.InfluxDb.Url, config.InfluxDb.Token);
			}
		);
	}
}