using System;
using System.Globalization;
using System.Text.Json;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
				return new InfluxDBClient(config.InfluxDb.Url, config.InfluxDb.Token);
			}
		);
	}
}

internal class Configuration
{
	public string DataSource { get; set; }
	public TimeSpan Repeat { get; set; }
	public InfluxDb InfluxDb { get; set; }
}

internal class InfluxDb
{
	public string Token { get; set; }
	public string Url { get; set; }
	public string Bucket { get; set; }
	public string Organisation { get; set; }
}

internal class BoMCollector
{
	private readonly HttpClient _client;
	private readonly IOptionsMonitor<Configuration> _configuration;
	private readonly ILogger<BoMCollector> _logger;
	private readonly InfluxDBClient _influxDbClient;

	public BoMCollector(InfluxDBClient influxDbClient, IOptionsMonitor<Configuration> configuration, ILogger<BoMCollector> logger)
	{
		_influxDbClient = influxDbClient;
		_configuration = configuration;
		_logger = logger;
		_client = new HttpClient();
		_client.DefaultRequestHeaders.Add("X-Scraper", "Nulah.BoMDataCollector");
		_client.DefaultRequestHeaders.Add("X-Source", "https://github.com/ZacMillionaire/Nulah.BoMDataCollector");
		_client.DefaultRequestHeaders.Add("X-Target-Interval", "30m");
	}


	public async Task ReadJson()
	{
		_logger.LogInformation("[{timestamp}] Collecting from data source: '{source}'", DateTime.Now, _configuration.CurrentValue.DataSource);
		var req = await _client.GetStreamAsync(_configuration.CurrentValue.DataSource);
		var res = JsonSerializer.Deserialize<BoMData>(req);

		if (res != null)
		{
			using var writeApi = _influxDbClient.GetWriteApi();

			var header = res.observations.header.FirstOrDefault();

			foreach (var observation in res.observations.data)
			{
				var measurementTime = DateTime.ParseExact(observation.aifstime_utc, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);

				// rain trace can also be a single dash in the response, but lets treat it as if it could be any invalid decimal
				// value instead and use a TryParse.
				// A single dash is not the same as a zero value for our use case (the data may come back as 0.0 as there
				// could have been a trace amount of rain somewhere but the json is only accurate to 1 decimal place
				//http://www.bom.gov.au/catalogue/observations/about-weather-observations.shtml
				var rainTraceParsed = decimal.TryParse(observation.rain_trace, out var rainTrace);

				var point = PointData.Measurement("weather-data").Tag("source", "Bureau of Meteorology")
					.Field("air_temp", observation.air_temp)
					.Field("apparent_t", observation.apparent_t)
					// this is also known as wet bulb
					.Field("delta_t", observation.delta_t)
					.Field("rel_hum", observation.rel_hum)
					.Field("rain_trace", rainTraceParsed ? rainTrace : null)
					// these 3 are probably all the same
					.Field("press", observation.press)
					.Field("press_msl", observation.press_msl)
					.Field("press_qnh", observation.press_qnh)
					.Field("wind_dir", observation.wind_dir)
					.Field("wind_spd_kmh", observation.wind_spd_kmh)
					.Field("gust_kmh", observation.gust_kmh)
					.Field("dewpt", observation.dewpt)
					.Timestamp(measurementTime, WritePrecision.S);

				// Add location Ids. The result from this is an array but appears to be a length of 1 so we check
				// that we have header details before adding them, just incase responses change in the future
				if (header != null)
				{
					point = point.Tag("ID", header.ID)
						.Tag("main_ID", header.main_ID);
				}

				writeApi.WritePoint(point, _configuration.CurrentValue.InfluxDb.Bucket, _configuration.CurrentValue.InfluxDb.Organisation);
			}
		}
	}


	#region Json Classes

	private class BoMData
	{
		public Observations observations { get; set; }
	}

	private class Observations
	{
		public Notice[] notice { get; set; }
		public Header[] header { get; set; }
		public Data[] data { get; set; }
	}

	private class Notice
	{
		public string copyright { get; set; }
		public string copyright_url { get; set; }
		public string disclaimer_url { get; set; }
		public string feedback_url { get; set; }
	}

	private class Header
	{
		public string refresh_message { get; set; }
		public string ID { get; set; }
		public string main_ID { get; set; }
		public string name { get; set; }
		public string state_time_zone { get; set; }
		public string time_zone { get; set; }
		public string product_name { get; set; }
		public string state { get; set; }
	}

	private class Data
	{
		public int? sort_order { get; set; }
		public int? wmo { get; set; }
		public string name { get; set; }
		public string history_product { get; set; }
		public string local_date_time { get; set; }
		public string local_date_time_full { get; set; }
		public string aifstime_utc { get; set; }
		public double? lat { get; set; }
		public double? lon { get; set; }
		public double? apparent_t { get; set; }
		public string cloud { get; set; }
		public object cloud_base_m { get; set; }
		public object cloud_oktas { get; set; }
		public object cloud_type_id { get; set; }
		public string cloud_type { get; set; }
		public double? delta_t { get; set; }
		public int? gust_kmh { get; set; }
		public int? gust_kt { get; set; }
		public double? air_temp { get; set; }
		public double? dewpt { get; set; }
		public double? press { get; set; }
		public double? press_qnh { get; set; }
		public double? press_msl { get; set; }
		public string press_tend { get; set; }
		public string rain_trace { get; set; }
		public int? rel_hum { get; set; }
		public string sea_state { get; set; }
		public string swell_dir_worded { get; set; }
		public object swell_height { get; set; }
		public object swell_period { get; set; }
		public string vis_km { get; set; }
		public string weather { get; set; }
		public string wind_dir { get; set; }
		public int? wind_spd_kmh { get; set; }
		public int? wind_spd_kt { get; set; }
	}

	#endregion
}

internal class CollectData : IHostedService, IDisposable
{
	private readonly BoMCollector _collector;
	private readonly ILogger<CollectData> _logger;
	private Timer? _timer = null;
	private readonly IOptionsMonitor<Configuration> _configuration;

	public CollectData(BoMCollector collector, ILogger<CollectData> logger, IOptionsMonitor<Configuration> configuration)
	{
		_collector = collector;
		_logger = logger;
		_configuration = configuration;
	}

	public Task StartAsync(CancellationToken stoppingToken)
	{
		_logger.LogInformation("Collect Data Service running.");

		_timer = new Timer(DoWork, null, TimeSpan.Zero,
			_configuration.CurrentValue.Repeat);


		return Task.CompletedTask;
	}

	public Task StopAsync(CancellationToken stoppingToken)
	{
		_logger.LogInformation("Collect Data Service is stopping.");

		return Task.CompletedTask;
	}

	public void Dispose()
	{
		_timer?.Dispose();
	}

	private async void DoWork(object? state)
	{
		_logger.LogInformation("Collect Data Service is working");
		await _collector.ReadJson();
	}
}